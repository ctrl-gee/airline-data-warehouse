# Airline Data Warehouse

An Airline Data Warehouse with Insurance Eligibility system built with Node.js, Express, Supabase, and Kafka.

## Features

- CSV file upload and processing for airline data
- Smart file type detection
- Flight delay monitoring via Kafka
- Insurance eligibility checking based on flight delays
- Dirty data tracking and monitoring

## Setup

1. Clone the repository
2. Run `npm install`
3. Set up environment variables:
   - `SUPABASE_URL` - Your Supabase project URL
   - `SUPABASE_ANON_KEY` - Your Supabase anonymous key
4. Run `npm start` to start the server

## API Endpoints

- `POST /upload-sales` - Upload sales CSV files
- `POST /test-auto-detect` - Test file type auto-detection
- `POST /check-insurance` - Check insurance eligibility
- `POST /simulate-delay` - Simulate flight delay (for testing)
- `GET /dirty-data` - Get dirty data records

## Built with Replit
