// uploadPassengers.js - FIXED VERSION
const { createClient } = require('@supabase/supabase-js');
const csv = require('csv-parser');
const fs = require('fs');
require('dotenv').config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

function standardizePassengerKey(inputKey) {
  if (!inputKey || typeof inputKey !== 'string') return null;

  // Remove any whitespace
  inputKey = inputKey.trim();

  // If already starts with P and has numbers, standardize
  if (inputKey.startsWith('P') && /\d/.test(inputKey)) {
    const numbersOnly = inputKey.replace(/\D/g, '');
    if (numbersOnly.length < 3) return null;

    const lastThree = numbersOnly.slice(-3);
    return `P${lastThree.padStart(3, '0')}`;
  }

  // If no P but has numbers, add P
  if (/\d/.test(inputKey)) {
    const numbersOnly = inputKey.replace(/\D/g, '');
    if (numbersOnly.length < 3) return null;

    const lastThree = numbersOnly.slice(-3);
    return `P${lastThree.padStart(3, '0')}`;
  }

  return null;
}

function standardizeLoyaltyStatus(status) {
  if (!status || typeof status !== 'string') return 'Bronze'; // Changed from 'NONE' to 'Bronze'

  const upperStatus = status.trim().toUpperCase();

  // Map to valid CHECK constraint values from your table
  if (upperStatus.includes('PLATINUM') || upperStatus === 'PLAT') return 'Platinum';
  if (upperStatus.includes('GOLD') || upperStatus === 'GOLD') return 'Gold';
  if (upperStatus.includes('SILVER') || upperStatus === 'SILV') return 'Silver';
  if (upperStatus.includes('BRONZE') || upperStatus === 'BRNZ') return 'Bronze';

  return 'Bronze'; // Default to Bronze to match CHECK constraint
}

function standardizeEmail(fullName, existingEmail) {
  if (existingEmail && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(existingEmail)) {
    return existingEmail.toLowerCase();
  }

  const names = fullName.trim().split(' ');
  const firstName = names[0].toLowerCase();
  const lastName = names.length > 1 ? names[names.length - 1].toLowerCase() : '';

  if (lastName) {
    return `${firstName}.${lastName}@example.com`;
  } else {
    return `${firstName}@example.com`;
  }
}

// NEW FUNCTION: Save to dirty table
async function saveToDirtyTable(rowData, errorReason) {
  try {
    const dirtyRecord = {
      source_table: 'passengers',
      original_data: rowData,
      error_reason: errorReason,
      created_at: new Date().toISOString()
    };

    const { error } = await supabase
      .from('dirty_data')
      .insert(dirtyRecord);

    if (error) {
      console.error('Failed to save to dirty table:', error.message);
      return false;
    }

    return true;
  } catch (err) {
    console.error('Error saving to dirty table:', err.message);
    return false;
  }
}

async function uploadPassengers() {
  console.log('Starting passenger upload...\n');

  // Step 1: Read CSV data
  const rawRows = [];
  console.log('Reading passengers.csv...');

  try {
    await new Promise((resolve, reject) => {
      fs.createReadStream('datasets/passengers.csv')
        .pipe(csv())
        .on('data', (row) => {
          rawRows.push(row);
        })
        .on('end', resolve)
        .on('error', reject);
    });
  } catch (error) {
    console.error('Error reading CSV:', error);
    return;
  }

  console.log(`Read ${rawRows.length} rows from CSV\n`);

  // Step 2: Process rows with standardized keys
  const passengerMap = new Map();
  const duplicates = [];
  const invalidKeys = [];

  for (let i = 0; i < rawRows.length; i++) {
    const row = rawRows[i];
    const originalKey = row.PassengerKey?.trim();

    // Standardize the passenger key
    const standardizedKey = standardizePassengerKey(originalKey);

    if (!standardizedKey) {
      invalidKeys.push({
        row: i + 1,
        originalKey: originalKey,
        name: row.FullName
      });
      // Save invalid key to dirty table
      await saveToDirtyTable(row, `Invalid passenger key format: ${originalKey}`);
      continue;
    }

    // Check for duplicate
    if (passengerMap.has(standardizedKey)) {
      duplicates.push({
        row: i + 1,
        originalKey: originalKey,
        standardizedKey: standardizedKey,
        name: row.FullName,
        firstRow: passengerMap.get(standardizedKey).rowNumber
      });
      // Save duplicate to dirty table
      await saveToDirtyTable(row, `Duplicate passenger key: ${standardizedKey}`);
      continue;
    }

    // Standardize loyalty status (now returns valid CHECK constraint values)
    const loyaltyStatus = standardizeLoyaltyStatus(row.LoyaltyStatus);

    // Store unique passenger with standardized data
    passengerMap.set(standardizedKey, {
      rowNumber: i + 1,
      originalKey: originalKey,
      data: {
        passenger_key: standardizedKey,
        full_name: row.FullName?.trim() || 'Unknown',
        email: standardizeEmail(row.FullName?.trim() || 'Unknown', row.Email?.trim()),
        loyalty_status: loyaltyStatus  // Now matches CHECK constraint
      }
    });
  }

  // Convert to array
  const uniquePassengers = Array.from(passengerMap.values()).map(item => item.data);

  console.log(`\n=== DATA SUMMARY ===`);
  console.log(`Total rows in CSV: ${rawRows.length}`);
  console.log(`Unique passengers: ${uniquePassengers.length}`);
  console.log(`Invalid keys moved to dirty table: ${invalidKeys.length}`);
  console.log(`Duplicates moved to dirty table: ${duplicates.length}`);

  if (invalidKeys.length > 0) {
    console.log('\n=== INVALID PASSENGER KEYS ===');
    invalidKeys.slice(0, 5).forEach(invalid => {
      console.log(`Row ${invalid.row}: "${invalid.originalKey}" (${invalid.name}) -> DIRTY TABLE`);
    });
    if (invalidKeys.length > 5) {
      console.log(`... and ${invalidKeys.length - 5} more in dirty table`);
    }
  }

  if (duplicates.length > 0) {
    console.log('\n=== DUPLICATES ===');
    duplicates.slice(0, 5).forEach(dup => {
      console.log(`Row ${dup.row}: "${dup.originalKey}" -> ${dup.standardizedKey} - DUPLICATE -> DIRTY TABLE`);
    });
    if (duplicates.length > 5) {
      console.log(`... and ${duplicates.length - 5} more in dirty table`);
    }
  }

  // Step 3: Upload clean passengers
  if (uniquePassengers.length > 0) {
    console.log('\nUploading clean passengers...');

    const batchSize = 20;
    let uploaded = 0;
    let errors = 0;
    let alreadyExist = 0;
    const failedPassengers = [];

    for (let i = 0; i < uniquePassengers.length; i += batchSize) {
      const batch = uniquePassengers.slice(i, i + batchSize);
      const batchNum = Math.floor(i / batchSize) + 1;

      console.log(`\nProcessing batch ${batchNum} (${batch.length} passengers)...`);

      try {
        // Try insert
        const { error } = await supabase
          .from('dim_passenger')
          .insert(batch)
          .select();

        if (error) {
          if (error.code === '23505' || error.message.includes('duplicate')) {
            console.log(`  Some duplicates exist, inserting individually...`);

            // Insert one by one
            for (const passenger of batch) {
              try {
                const { error: singleError } = await supabase
                  .from('dim_passenger')
                  .insert(passenger)
                  .select();

                if (singleError) {
                  if (singleError.code === '23505' || singleError.message.includes('duplicate')) {
                    console.log(`    ⚠️  ${passenger.passenger_key}: Already exists`);
                    alreadyExist++;
                  } else {
                    console.error(`    ✗ ${passenger.passenger_key}:`, singleError.message);
                    // Save failed passenger to dirty table
                    await saveToDirtyTable(passenger, `Upload failed: ${singleError.message}`);
                    failedPassengers.push(passenger.passenger_key);
                    errors++;
                  }
                } else {
                  uploaded++;
                  console.log(`    ✓ ${passenger.passenger_key}: Added`);
                }
              } catch (singleErr) {
                console.error(`    ✗ ${passenger.passenger_key}:`, singleErr.message);
                // Save failed passenger to dirty table
                await saveToDirtyTable(passenger, `Upload error: ${singleErr.message}`);
                failedPassengers.push(passenger.passenger_key);
                errors++;
              }

              // Small delay
              await new Promise(resolve => setTimeout(resolve, 50));
            }
          } else {
            console.error(`  Batch error:`, error.message);

            // Save entire batch to dirty table
            for (const passenger of batch) {
              await saveToDirtyTable(passenger, `Batch upload failed: ${error.message}`);
              failedPassengers.push(passenger.passenger_key);
            }
            errors += batch.length;
          }
        } else {
          uploaded += batch.length;
          console.log(`  ✓ Batch ${batchNum}: ${batch.length} passengers added`);
        }

        // Small delay between batches
        await new Promise(resolve => setTimeout(resolve, 300));

      } catch (batchError) {
        console.error(`  ✗ Batch ${batchNum} error:`, batchError.message);
        errors += batch.length;
      }
    }

    console.log(`\n✅ UPLOAD SUMMARY`);
    console.log(`Successfully uploaded: ${uploaded} passengers`);
    console.log(`Already existed: ${alreadyExist}`);
    console.log(`Errors (moved to dirty): ${errors}`);
    console.log(`Total processed: ${uploaded + alreadyExist + errors}/${uniquePassengers.length}`);

    // Verify counts
    try {
      const { count } = await supabase
        .from('dim_passenger')
        .select('*', { count: 'exact', head: true });

      console.log(`\nTotal passengers in dim_passenger: ${count}`);

      const { count: dirtyCount } = await supabase
        .from('dirty_data')
        .select('*', { count: 'exact', head: true });

      console.log(`Total records in dirty_data: ${dirtyCount}`);

    } catch (countErr) {
      console.log('Count verification error:', countErr.message);
    }

  } else {
    console.log('No valid passengers to upload - all data was invalid or moved to dirty table');
  }
}

uploadPassengers();