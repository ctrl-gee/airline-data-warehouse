const express = require("express");
const multer = require("multer");
const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");
const smartFileProcessor = require("./smartFileProcessor");
const { KafkaService } = require("./kafkaConfig");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.static("public"));
app.use(express.urlencoded({ extended: true }));

// Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY,
);

// Multer configuration for file uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, "uploads/");
  },
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}-${file.originalname}`);
  },
});

const upload = multer({
  storage: storage,
  fileFilter: (req, file, cb) => {
    if (file.mimetype === "text/csv" || file.originalname.endsWith(".csv")) {
      cb(null, true);
    } else {
      cb(new Error("Only CSV files are allowed"));
    }
  },
});

// Ensure uploads directory exists
if (!fs.existsSync("uploads")) {
  fs.mkdirSync("uploads");
}

// Routes
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// In server.js - Updated upload endpoint
app.post('/upload-sales', upload.single('file'), async (req, res) => {
  try {
    const { sourceType } = req.body; // 'travel_agency' or 'corporate'

    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const results = [];

    // Read CSV file
    await new Promise((resolve, reject) => {
      fs.createReadStream(req.file.path)
        .pipe(csv())
        .on('data', (data) => results.push(data))
        .on('end', resolve)
        .on('error', reject);
    });

    // Process based on source type
    const processedData = await DataProcessor.processSalesData(
      results, 
      sourceType
    );

    // Insert into fact_sales table
    if (processedData.length > 0) {
      const { error } = await supabase
        .from('fact_sales')
        .insert(processedData);

      if (error) throw error;
    }

    // Clean up
    fs.unlinkSync(req.file.path);

    res.json({
      success: true,
      source: sourceType,
      totalRecords: results.length,
      cleanRecords: processedData.length,
      dirtyRecords: results.length - processedData.length
    });

  } catch (error) {
    console.error('Sales upload error:', error);
    res.status(500).json({ error: 'Failed to process sales data' });
  }
});

// In server.js - Add test endpoint
app.post('/test-auto-detect', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    // Read headers
    const headers = await SmartFileProcessor.readCSVHeaders(req.file.path);

    // Read sample data
    const data = await SmartFileProcessor.readCSVData(req.file.path);

    // Detect type
    const fileType = SmartFileProcessor.detectFileType(headers);

    // Clean up
    fs.unlinkSync(req.file.path);

    res.json({
      filename: req.file.originalname,
      fileType,
      headers,
      sampleRows: data.slice(0, 3), // First 3 rows
      totalRows: data.length,
      suggestedAction: fileType === 'unknown' ? 
        'Manual review needed' : 
        `Will be processed as ${fileType}`
    });

  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Insurance eligibility check endpoint
app.post("/check-insurance", async (req, res) => {
  try {
    const { passengerName, flightId, baggage, date } = req.body;

    if (!flightId) {
      return res.status(400).json({ error: "Flight ID is required" });
    }

    // Check flight delay status
    const { data: flightStatus, error } = await supabase
      .from("flight_status_updates")
      .select("*")
      .eq("flight_key", flightId)
      .order("update_timestamp", { ascending: false })
      .limit(1);

    if (error) throw error;

    const isEligible =
      flightStatus.length > 0 && flightStatus[0].delay_minutes > 240; // 4 hours

    // Update insurance eligibility in sales data
    if (isEligible) {
      await supabase
        .from("fact_sales")
        .update({ is_eligible_insurance: true })
        .eq("flight_key", flightId);
    }

    res.json({
      eligible: isEligible,
      message: isEligible
        ? "Customer is ELIGIBLE for Insurance"
        : "Customer is NOT ELIGIBLE for Insurance",
      delayMinutes: flightStatus.length > 0 ? flightStatus[0].delay_minutes : 0,
    });
  } catch (error) {
    console.error("Insurance check error:", error);
    res.status(500).json({ error: "Failed to check insurance eligibility" });
  }
});

// Manually trigger flight delay (for testing)
app.post("/simulate-delay", async (req, res) => {
  try {
    const { flightKey, delayMinutes } = req.body;

    const flightUpdate = {
      flight_key: flightKey,
      status: delayMinutes > 0 ? "delayed" : "on-time",
      delay_minutes: delayMinutes || 0,
      update_timestamp: new Date().toISOString(),
    };

    // Send to Kafka
    await KafkaService.sendFlightUpdate(flightUpdate);

    res.json({
      message: "Flight delay simulation sent to Kafka",
      flightUpdate,
    });
  } catch (error) {
    console.error("Delay simulation error:", error);
    res.status(500).json({ error: "Failed to simulate delay" });
  }
});

// Get dirty data for monitoring
app.get("/dirty-data", async (req, res) => {
  try {
    const { data, error } = await supabase
      .from("dirty_data")
      .select("*")
      .order("created_at", { ascending: false })
      .limit(100);

    if (error) throw error;

    res.json(data);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch dirty data" });
  }
});

// Initialize Kafka and start server
async function startServer() {
  try {
    await KafkaService.connect();

    // Start listening for flight delays
    await KafkaService.listenForFlightDelays(async (flightData) => {
      console.log(`Processing flight update: ${flightData.flight_key}`);

      // Insert into flight_status_updates table
      const { error } = await supabase
        .from("flight_status_updates")
        .insert(flightData);

      if (error) {
        console.error("Error saving flight update:", error);
      }
    });

    app.listen(port, () => {
      console.log(`Server running on port ${port}`);
    });
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
}

startServer();
