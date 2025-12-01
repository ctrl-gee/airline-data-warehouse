// smartFileProcessor.js
const { createClient } = require('@supabase/supabase-js');
const csv = require('csv-parser');
const fs = require('fs');
require('dotenv').config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

class SmartFileProcessor {

  // File type signatures (column patterns)
  static fileSignatures = {
    passengers: {
      requiredColumns: ['PassengerKey', 'FullName'],
      optionalColumns: ['Email', 'LoyaltyStatus'],
      targetTable: 'dim_passenger',
      processor: 'processPassengerData'
    },
    airports: {
      requiredColumns: ['AirportKey', 'AirportName', 'City', 'Country'],
      targetTable: 'dim_airport',
      processor: 'processAirportData'
    },
    airlines: {
      requiredColumns: ['AirlineKey', 'AirlineName'],
      optionalColumns: ['Alliance'],
      targetTable: 'dim_airline',
      processor: 'processAirlineData'
    },
    flights: {
      requiredColumns: ['FlightKey', 'OriginAirportKey', 'DestinationAirportKey'],
      optionalColumns: ['AircraftType'],
      targetTable: 'dim_flight',
      processor: 'processFlightData'
    },
    travel_agency_sales: {
      requiredColumns: ['TransactionID', 'TransactionDate', 'PassengerID', 'FlightID'],
      amountColumns: ['TicketPrice', 'Taxes', 'BaggageFees', 'TotalAmount'],
      targetTable: 'fact_sales',
      processor: 'processSalesData',
      sourceType: 'travel_agency'
    },
    corporate_sales: {
      requiredColumns: ['TransactionID', 'DateKey', 'PassengerKey', 'FlightKey'],
      amountColumns: ['TicketPrice', 'Taxes', 'BaggageFees', 'TotalAmount'],
      targetTable: 'fact_sales',
      processor: 'processSalesData',
      sourceType: 'corporate'
    }
  };

  // Detect file type from columns
  static detectFileType(headers) {
    console.log('Detecting file type for headers:', headers);

    const lowerHeaders = headers.map(h => h.trim());

    for (const [fileType, signature] of Object.entries(this.fileSignatures)) {
      const hasRequired = signature.requiredColumns.every(col => 
        lowerHeaders.includes(col.toLowerCase())
      );

      // Special handling for sales files
      if (fileType.includes('sales')) {
        const hasAmountColumns = signature.amountColumns?.some(col =>
          lowerHeaders.includes(col.toLowerCase())
        );

        if (hasRequired && hasAmountColumns) {
          console.log(`Detected as: ${fileType}`);
          return fileType;
        }
      } else if (hasRequired) {
        console.log(`Detected as: ${fileType}`);
        return fileType;
      }
    }

    // Try fuzzy matching for common variations
    if (headers.some(h => h.toLowerCase().includes('passenger'))) {
      return 'passengers';
    } else if (headers.some(h => h.toLowerCase().includes('airport'))) {
      return 'airports';
    } else if (headers.some(h => h.toLowerCase().includes('airline'))) {
      return 'airlines';
    } else if (headers.some(h => h.toLowerCase().includes('flight'))) {
      return 'flights';
    } else if (headers.some(h => h.toLowerCase().includes('transaction'))) {
      // Determine which sales type
      if (headers.some(h => h.toLowerCase().includes('travel') || 
                            h.toLowerCase().includes('agency'))) {
        return 'travel_agency_sales';
      } else if (headers.some(h => h.toLowerCase().includes('corporate') ||
                                   h.toLowerCase().includes('datekey'))) {
        return 'corporate_sales';
      }
      return 'travel_agency_sales'; // default
    }

    return 'unknown';
  }

  // Read CSV headers only
  static async readCSVHeaders(filePath) {
    return new Promise((resolve, reject) => {
      const headers = [];
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('headers', (csvHeaders) => {
          resolve(csvHeaders);
        })
        .on('error', reject)
        .on('data', () => {
          // We only need headers, stop after first row
          resolve(headers);
        });
    });
  }

  // Process file based on detected type
  static async processFile(filePath, originalFilename) {
    try {
      // Read headers to detect file type
      const headers = await this.readCSVHeaders(filePath);
      const fileType = this.detectFileType(headers);

      if (fileType === 'unknown') {
        throw new Error(`Cannot detect file type. Headers: ${headers.join(', ')}`);
      }

      console.log(`File "${originalFilename}" detected as: ${fileType}`);

      // Read all data
      const data = await this.readCSVData(filePath);

      // Process based on file type
      let result;
      const signature = this.fileSignatures[fileType];

      switch (fileType) {
        case 'passengers':
          result = await this.processPassengerData(data);
          break;
        case 'airports':
          result = await this.processAirportData(data);
          break;
        case 'airlines':
          result = await this.processAirlineData(data);
          break;
        case 'flights':
          result = await this.processFlightData(data);
          break;
        case 'travel_agency_sales':
          result = await this.processSalesData(data, 'travel_agency');
          break;
        case 'corporate_sales':
          result = await this.processSalesData(data, 'corporate');
          break;
        default:
          throw new Error(`No processor for file type: ${fileType}`);
      }

      // Insert into database
      if (result.cleanData && result.cleanData.length > 0) {
        const { error } = await supabase
          .from(signature.targetTable)
          .upsert(result.cleanData, { 
            onConflict: this.getConflictColumn(signature.targetTable)
          });

        if (error) throw error;
      }

      // Save dirty data
      if (result.dirtyData && result.dirtyData.length > 0) {
        await this.saveDirtyData(result.dirtyData, fileType);
      }

      return {
        success: true,
        fileType,
        originalFilename,
        totalRecords: data.length,
        cleanRecords: result.cleanData?.length || 0,
        dirtyRecords: result.dirtyData?.length || 0,
        detectionConfidence: 'high'
      };

    } catch (error) {
      console.error('File processing error:', error);
      return {
        success: false,
        error: error.message,
        originalFilename
      };
    }
  }

  static getConflictColumn(tableName) {
    const conflictMap = {
      'dim_passenger': 'passenger_key',
      'dim_airport': 'airport_key',
      'dim_airline': 'airline_key',
      'dim_flight': 'flight_key',
      'fact_sales': 'transaction_id'
    };
    return conflictMap[tableName] || 'id';
  }

  static async readCSVData(filePath) {
    return new Promise((resolve, reject) => {
      const data = [];
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => data.push(row))
        .on('end', () => resolve(data))
        .on('error', reject);
    });
  }

  // Process passenger data
  static async processPassengerData(data) {
    const cleanData = [];
    const dirtyData = [];

    for (const row of data) {
      try {
        // Standardize passenger key
        const passengerKey = this.standardizePassengerKey(row.PassengerKey);
        if (!passengerKey) {
          dirtyData.push({ row, reason: 'Invalid passenger key' });
          continue;
        }

        // Standardize email
        const email = this.standardizeEmail(row.FullName, row.Email);

        cleanData.push({
          passenger_key: passengerKey,
          full_name: row.FullName?.trim(),
          email: email,
          loyalty_status: row.LoyaltyStatus?.trim() || 'Bronze'
        });
      } catch (error) {
        dirtyData.push({ row, reason: `Processing error: ${error.message}` });
      }
    }

    return { cleanData, dirtyData };
  }

  // Process airport data with hierarchy
  static async processAirportData(data) {
    const cleanData = [];
    const dirtyData = [];

    for (const row of data) {
      try {
        const airportKey = row.AirportKey?.trim().toUpperCase();
        if (!airportKey || airportKey.length !== 3) {
          dirtyData.push({ row, reason: 'Invalid airport key' });
          continue;
        }

        // Standardize country
        const country = await this.standardizeCountry(row.Country, row.City);

        // Get country_id from hierarchy
        const { data: countryData } = await supabase
          .from('dim_country_hierarchy')
          .select('country_id')
          .eq('country_name', country)
          .single();

        cleanData.push({
          airport_key: airportKey,
          airport_name: row.AirportName?.trim(),
          city: row.City?.trim(),
          country: country,
          country_id: countryData?.country_id,
          hierarchy_processed: true
        });
      } catch (error) {
        dirtyData.push({ row, reason: error.message });
      }
    }

    return { cleanData, dirtyData };
  }

  // Standardize passenger key (P001 format)
  static standardizePassengerKey(inputKey) {
    if (!inputKey || typeof inputKey !== 'string') return null;
    if (!inputKey.includes('P')) return null;
    const numbers = inputKey.replace(/\D/g, '');
    if (numbers.length < 3) return null;
    return 'P' + numbers.slice(-3).padStart(3, '0');
  }

  // Standardize email
  static standardizeEmail(fullName, existingEmail) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (existingEmail && emailRegex.test(existingEmail)) {
      return existingEmail.toLowerCase();
    }

    const names = fullName?.trim().split(' ') || [];
    const firstName = names[0]?.toLowerCase() || 'user';
    const lastName = names.length > 1 ? names[names.length - 1].toLowerCase() : '';

    return lastName ? 
      `${firstName}.${lastName}@example.com` : 
      `${firstName}@example.com`;
  }

  // Standardize country
  static async standardizeCountry(country, city) {
    const countryMap = {
      'us': 'United States',
      'usa': 'United States',
      'united states': 'United States',
      'u.s.a.': 'United States',
      'u.s.': 'United States',
      'uk': 'United Kingdom',
      'united kingdom': 'United Kingdom',
      'great britain': 'United Kingdom',
      'uae': 'United Arab Emirates',
      'united arab emirates': 'United Arab Emirates',
      'u.a.e.': 'United Arab Emirates'
    };

    const normalized = country?.toLowerCase().trim() || '';
    if (countryMap[normalized]) {
      return countryMap[normalized];
    }

    // Check if country exists in hierarchy
    const { data: existing } = await supabase
      .from('dim_country_hierarchy')
      .select('country_name')
      .ilike('country_name', `%${normalized}%`)
      .limit(1);

    return existing?.[0]?.country_name || 
           country?.charAt(0).toUpperCase() + country?.slice(1).toLowerCase() || 
           'Unknown';
  }

  // Process sales data
  static async processSalesData(data, sourceType) {
    const cleanData = [];
    const dirtyData = [];

    for (const row of data) {
      try {
        // Standardize passenger key
        const passengerKey = row.PassengerID || row.PassengerKey;
        const standardizedKey = this.standardizePassengerKey(passengerKey);

        if (!standardizedKey) {
          dirtyData.push({ row, reason: 'Invalid passenger key' });
          continue;
        }

        // Get flight key
        const flightKey = row.FlightID || row.FlightKey;
        if (!flightKey) {
          dirtyData.push({ row, reason: 'Missing flight key' });
          continue;
        }

        // Standardize date
        const dateStr = row.TransactionDate || row.DateKey;
        const date = this.standardizeDate(dateStr);
        if (!date) {
          dirtyData.push({ row, reason: 'Invalid date' });
          continue;
        }

        const dateKey = parseInt(date.replace(/-/g, ''));

        // Create transaction ID with prefix
        const transId = row.TransactionID?.toString().trim();
        const transactionId = sourceType === 'travel_agency' ?
          'TA' + transId.replace(/\D/g, '').padStart(6, '0') :
          'CO' + transId.replace(/\D/g, '').padStart(6, '0');

        // Standardize amounts
        const standardize = (amount) => {
          const num = parseFloat(String(amount).replace(/[^\d.-]/g, ''));
          return isNaN(num) ? 0.00 : parseFloat(num.toFixed(2));
        };

        cleanData.push({
          transaction_id: transactionId,
          date_key: dateKey,
          passenger_key: standardizedKey,
          flight_key: flightKey,
          ticket_price: standardize(row.TicketPrice),
          taxes: standardize(row.Taxes),
          baggage_fees: standardize(row.BaggageFees),
          total_amount: standardize(row.TotalAmount),
          sales_source: sourceType
        });

      } catch (error) {
        dirtyData.push({ row, reason: `Processing error: ${error.message}` });
      }
    }

    return { cleanData, dirtyData };
  }

  static standardizeDate(dateStr) {
    try {
      const date = new Date(dateStr);
      if (isNaN(date.getTime())) return null;
      return date.toISOString().split('T')[0]; // YYYY-MM-DD
    } catch {
      return null;
    }
  }

  // Save dirty data
  static async saveDirtyData(dirtyRows, sourceTable) {
    const dirtyRecords = dirtyRows.map(item => ({
      source_table: sourceTable,
      original_data: item.row,
      error_reason: item.reason,
      created_at: new Date().toISOString()
    }));

    if (dirtyRecords.length > 0) {
      await supabase
        .from('dirty_data')
        .insert(dirtyRecords);
    }
  }

  // Process airline data
  static async processAirlineData(data) {
    const cleanData = data.map(row => ({
      airline_key: row.AirlineKey?.trim().toUpperCase(),
      airline_name: row.AirlineName?.trim(),
      alliance: row.Alliance?.trim() === 'N/A' ? null : row.Alliance?.trim()
    })).filter(row => row.airline_key);

    return { cleanData, dirtyData: [] };
  }

  // Process flight data
  static async processFlightData(data) {
    const cleanData = [];
    const dirtyData = [];

    for (const row of data) {
      try {
        const flightKey = row.FlightKey?.trim();
        const origin = row.OriginAirportKey?.trim().toUpperCase();
        const destination = row.DestinationAirportKey?.trim().toUpperCase();

        if (!flightKey || !origin || !destination) {
          dirtyData.push({ row, reason: 'Missing required flight data' });
          continue;
        }

        cleanData.push({
          flight_key: flightKey,
          origin_airport_key: origin,
          destination_airport_key: destination,
          aircraft_type: row.AircraftType?.trim()
        });
      } catch (error) {
        dirtyData.push({ row, reason: error.message });
      }
    }

    return { cleanData, dirtyData };
  }

  // Batch process multiple files
  static async processMultipleFiles(files) {
    const results = [];

    for (const file of files) {
      console.log(`Processing: ${file.originalname}`);
      const result = await this.processFile(file.path, file.originalname);
      results.push(result);

      // Clean up file
      if (fs.existsSync(file.path)) {
        fs.unlinkSync(file.path);
      }
    }

    return results;
  }
}

module.exports = SmartFileProcessor;