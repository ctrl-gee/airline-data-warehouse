// uploadFlights.js - FIXED VERSION
const { createClient } = require("@supabase/supabase-js");
const csv = require("csv-parser");
const fs = require("fs");
require("dotenv").config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY,
);

// Cache for airports
const airportCache = new Set();

async function getOrCreateAirport(code) {
  if (airportCache.has(code)) {
    return true;
  }

  const { data: airport } = await supabase
    .from("dim_airport")
    .select("airport_key")
    .eq("airport_key", code)
    .single();

  if (airport) {
    airportCache.add(code);
    return true;
  }

  // Create placeholder
  console.log(`Creating placeholder for ${code}`);
  const { error } = await supabase.from("dim_airport").upsert(
    {
      airport_key: code,
      airport_name: `${code} Airport`,
      city: "Unknown",
      country: "Unknown",
    },
    { onConflict: "airport_key" },
  );

  if (error) {
    console.error(`Error creating airport ${code}:`, error.message);
    return false;
  }

  airportCache.add(code);
  return true;
}

async function uploadFlights() {
  console.log("Starting flight upload...\n");

  // Step 1: Load existing airports into cache
  console.log("Loading existing airports...");
  const { data: existingAirports } = await supabase
    .from("dim_airport")
    .select("airport_key");

  if (existingAirports) {
    existingAirports.forEach((a) => airportCache.add(a.airport_key));
    console.log(`Loaded ${airportCache.size} airports into cache\n`);
  }

  // Step 2: Read ALL CSV data first
  const rawRows = [];
  console.log("Reading flights.csv...");

  await new Promise((resolve, reject) => {
    fs.createReadStream("datasets/flights.csv")
      .pipe(csv())
      .on("data", (row) => {
        rawRows.push(row);
      })
      .on("end", resolve)
      .on("error", reject);
  });

  console.log(`Read ${rawRows.length} rows from CSV\n`);

  // Step 3: Process rows sequentially
  const flights = [];
  let skipped = 0;

  for (let i = 0; i < rawRows.length; i++) {
    const row = rawRows[i];
    const flightKey = row.FlightKey?.trim();
    const originCode = row.OriginAirportKey?.trim().toUpperCase();
    const destinationCode = row.DestinationAirportKey?.trim().toUpperCase();

    // Log first 5 rows
    if (i < 5) {
      console.log(
        `Processing ${i + 1}: ${flightKey} | ${originCode} -> ${destinationCode}`,
      );
    }

    // Skip if missing data
    if (!originCode || !destinationCode || !flightKey) {
      console.log(`Row ${i + 1}: Missing data, skipping`);
      skipped++;
      continue;
    }

    // Ensure airports exist
    const originOk = await getOrCreateAirport(originCode);
    const destOk = await getOrCreateAirport(destinationCode);

    if (!originOk || !destOk) {
      console.log(`✗ ${flightKey}: Airport creation failed`);
      skipped++;
      continue;
    }

    // Verify airports exist in DB
    const { data: originAirport } = await supabase
      .from("dim_airport")
      .select("airport_key")
      .eq("airport_key", originCode)
      .single();

    const { data: destAirport } = await supabase
      .from("dim_airport")
      .select("airport_key")
      .eq("airport_key", destinationCode)
      .single();

    if (!originAirport || !destAirport) {
      console.log(
        `✗ ${flightKey}: Verification failed for ${originCode}->${destinationCode}`,
      );
      skipped++;
      continue;
    }

    // Add flight
    flights.push({
      flight_key: flightKey,
      origin_airport_key: originCode,
      destination_airport_key: destinationCode,
      aircraft_type: row.AircraftType?.trim() || "Unknown",
    });

    if (i < 5) {
      console.log(`✓ ${flightKey}: Added to upload queue\n`);
    }
  }

  console.log(`\n=== PROCESSING COMPLETE ===`);
  console.log(`Total rows: ${rawRows.length}`);
  console.log(`Valid flights: ${flights.length}`);
  console.log(`Skipped: ${skipped}`);

  // Step 4: Upload flights
  if (flights.length > 0) {
    console.log("\nUploading flights to Supabase...");

    const batchSize = 50;
    let uploaded = 0;

    for (let i = 0; i < flights.length; i += batchSize) {
      const batch = flights.slice(i, i + batchSize);
      const batchNum = Math.floor(i / batchSize) + 1;

      const { error } = await supabase
        .from("dim_flight")
        .upsert(batch, { onConflict: "flight_key" });

      if (error) {
        console.error(`✗ Batch ${batchNum} error:`, error.message);

        // Try one by one
        for (const flight of batch) {
          const { error: singleError } = await supabase
            .from("dim_flight")
            .upsert(flight, { onConflict: "flight_key" });

          if (singleError) {
            console.error(
              `  Failed ${flight.flight_key}:`,
              singleError.message,
            );
          } else {
            uploaded++;
          }
        }
      } else {
        uploaded += batch.length;
        console.log(`✓ Batch ${batchNum}: ${batch.length} flights uploaded`);
      }
    }

    console.log(
      `\n✅ UPLOAD COMPLETE: ${uploaded}/${flights.length} flights uploaded`,
    );
  } else {
    console.log("\n❌ No flights to upload");

    // Debug: Check first few rows in detail
    console.log("\n=== DEBUG: First 5 rows ===");
    for (let i = 0; i < Math.min(5, rawRows.length); i++) {
      const row = rawRows[i];
      console.log(`Row ${i + 1}:`);
      console.log(`  FlightKey: "${row.FlightKey}"`);
      console.log(`  OriginAirportKey: "${row.OriginAirportKey}"`);
      console.log(`  DestinationAirportKey: "${row.DestinationAirportKey}"`);
      console.log(`  AircraftType: "${row.AircraftType}"`);
      console.log("");
    }

    // Check column names
    if (rawRows.length > 0) {
      console.log("=== Column names in CSV ===");
      console.log(Object.keys(rawRows[0]));
    }
  }
}

uploadFlights();
