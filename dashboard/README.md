# Semiconductor Parser Dashboard

This dashboard reads directly from the MySQL database produced by the parser.

## Features
- Read-only dashboard for the 5 main parser tables
- Summary cards and charts
- Client-side filtering and sorting
- Optional AI analysis for the currently visible tab
- No SQLite, no seed SQL, no in-memory DB

## Expected parser tables
- files
- equipment_states
- process_parameters_recipes
- sensor_readings
- fault_events
- wafer_processing_sequences
- rejected_records
- generic_observations_staging

## Setup
1. Copy `.env.example` to `.env`
2. Fill in your MySQL credentials
3. Install packages:
   npm install
4. Start:
   npm start
5. Open:
   http://localhost:3000

## Notes
- This version is read-only on purpose for stability.
- It reads the parser's current lean MySQL schema directly, so no SQL compatibility views are needed.
