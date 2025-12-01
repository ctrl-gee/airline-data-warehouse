// uploadAirports.js
const { createClient } = require('@supabase/supabase-js');
const csv = require('csv-parser');
const fs = require('fs');
require('dotenv').config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

async function uploadAirports() {
  const airports = [];
  const airportSet = new Set(); // Track unique airport keys

  console.log('Reading airports.csv...');

  // Read airports.csv
  await new Promise((resolve, reject) => {
    fs.createReadStream('datasets/airports.csv')
      .pipe(csv())
      .on('data', (row) => {
        // Clean and validate airport key
        const airportKey = row.AirportKey?.trim().toUpperCase();
        if (!airportKey || airportKey.length !== 3) {
          console.log(`⚠️  Skipping invalid airport key: "${row.AirportKey}"`);
          return;
        }

        // Check for duplicates
        if (airportSet.has(airportKey)) {
          console.log(`⚠️  Skipping duplicate airport: ${airportKey}`);
          return;
        }

        airportSet.add(airportKey);

        // Standardize country
        const country = standardizeCountry(row.Country);

        airports.push({
          airport_key: airportKey,
          airport_name: row.AirportName?.trim(),
          city: row.City?.trim(),
          country: country
        });
      })
      .on('end', () => {
        console.log(`📊 Read ${airports.length} unique airports`);
        resolve();
      })
      .on('error', reject);
  });

  if (airports.length === 0) {
    console.log('❌ No valid airports to upload');
    return;
  }

  console.log('Starting upload...');

  // Insert airports ONE BY ONE to avoid duplicate conflict
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < airports.length; i++) {
    const airport = airports[i];

    try {
      const { error } = await supabase
        .from('dim_airport')
        .upsert([airport], { 
          onConflict: 'airport_key',
          ignoreDuplicates: false // Update if exists
        });

      if (error) {
        console.error(`❌ Error uploading ${airport.airport_key}:`, error.message);
        errorCount++;
      } else {
        successCount++;
        if (successCount % 10 === 0) {
          console.log(`✅ Uploaded ${successCount}/${airports.length} airports...`);
        }
      }
    } catch (error) {
      console.error(`❌ Failed to upload ${airport.airport_key}:`, error.message);
      errorCount++;
    }

    // Small delay to avoid rate limiting
    if (i % 50 === 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  console.log('\n' + '='.repeat(50));
  console.log(`📊 UPLOAD SUMMARY:`);
  console.log(`✅ Successfully uploaded: ${successCount}`);
  console.log(`❌ Failed to upload: ${errorCount}`);
  console.log(`📁 Total processed: ${airports.length}`);

  if (errorCount > 0) {
    console.log('\n⚠️  Some airports failed to upload. Check the errors above.');
  } else {
    console.log('\n🎉 All airports uploaded successfully!');
  }
}

function standardizeCountry(country) {
  if (!country) return 'Unknown';

  const countryUpper = country.toUpperCase().trim();

  const countryMap = {
    'US': 'United States',
    'USA': 'United States',
    'UNITED STATES': 'United States',
    'UNITED STATES OF AMERICA': 'United States',
    'U.S.A.': 'United States',
    'U.S.': 'United States',
    'UK': 'United Kingdom',
    'UNITED KINGDOM': 'United Kingdom',
    'GREAT BRITAIN': 'United Kingdom',
    'ENGLAND': 'United Kingdom',
    'UAE': 'United Arab Emirates',
    'UNITED ARAB EMIRATES': 'United Arab Emirates',
    'JAPAN': 'Japan',
    'CANADA': 'Canada',
    'AUSTRALIA': 'Australia',
    'GERMANY': 'Germany',
    'FRANCE': 'France',
    'MEXICO': 'Mexico',
    'CHINA': 'China',
    'BRAZIL': 'Brazil',
    'INDIA': 'India'
  };

  return countryMap[countryUpper] || 
         countryUpper.charAt(0) + countryUpper.slice(1).toLowerCase();
}

// Run the upload
uploadAirports().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});