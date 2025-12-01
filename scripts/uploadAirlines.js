// uploadAirlines.js
const { createClient } = require('@supabase/supabase-js');
const csv = require('csv-parser');
const fs = require('fs');
require('dotenv').config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

async function uploadAirlines() {
  const airlines = [];

  await new Promise((resolve, reject) => {
    fs.createReadStream('datasets/airlines.csv')
      .pipe(csv())
      .on('data', (row) => {
        const airlineKey = row.AirlineKey?.trim().toUpperCase();
        if (!airlineKey) return;

        airlines.push({
          airline_key: airlineKey,
          airline_name: row.AirlineName?.trim(),
          alliance: row.Alliance?.trim() === 'N/A' ? null : row.Alliance?.trim()
        });
      })
      .on('end', resolve)
      .on('error', reject);
  });

  const { data, error } = await supabase
    .from('dim_airline')
    .upsert(airlines, { onConflict: 'airline_key' });

  if (error) {
    console.error('Error uploading airlines:', error);
  } else {
    console.log(`Uploaded ${airlines.length} airlines`);
  }
}

uploadAirlines();