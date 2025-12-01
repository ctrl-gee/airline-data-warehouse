// checkDuplicates.js
const csv = require('csv-parser');
const fs = require('fs');

console.log('🔍 Checking for duplicate airport codes...\n');

const airports = new Map();
const duplicates = [];

fs.createReadStream('datasets/airports.csv')
  .pipe(csv())
  .on('data', (row) => {
    const airportKey = row.AirportKey?.trim().toUpperCase();

    if (!airportKey || airportKey.length !== 3) {
      console.log(`⚠️  Invalid airport key: "${row.AirportKey}"`);
      return;
    }

    if (airports.has(airportKey)) {
      duplicates.push({
        key: airportKey,
        first: airports.get(airportKey),
        current: row
      });
    } else {
      airports.set(airportKey, {
        name: row.AirportName,
        city: row.City,
        country: row.Country
      });
    }
  })
  .on('end', () => {
    console.log(`📊 Total unique airports: ${airports.size}`);
    console.log(`⚠️  Duplicate airport codes found: ${duplicates.length}\n`);

    if (duplicates.length > 0) {
      console.log('='.repeat(60));
      console.log('DUPLICATE AIRPORT CODES:');
      console.log('='.repeat(60));

      duplicates.forEach((dup, index) => {
        console.log(`\n${index + 1}. Airport Code: ${dup.key}`);
        console.log(`   First occurrence: ${dup.first.name} - ${dup.first.city}, ${dup.first.country}`);
        console.log(`   Duplicate: ${dup.current.AirportName} - ${dup.current.City}, ${dup.current.Country}`);
      });

      console.log('\n💡 Recommendation: Remove or fix duplicates in your CSV file.');
    } else {
      console.log('✅ No duplicate airport codes found!');
    }
  });