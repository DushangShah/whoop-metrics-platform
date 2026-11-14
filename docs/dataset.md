# PMData dataset notes

Source: https://datasets.simula.no/pmdata (checked 2026-11-02)
Citation: Thambawita et al., "PMData: A Sports Logging Dataset", ACM MMSys 2020, DOI 10.1145/3339825.3394926
License: CC BY 4.0 per the source page, so credit the authors when reusing. A third-party mirror lists a non-commercial variant, so recheck before any commercial use.
Raw files are NOT stored in this repo. They live outside git and are linked at data/raw/pmdata.

## What was downloaded
- 16 participant folders (p01 to p16) plus participant-overview.xlsx at the top level
- About 3.0 GB after unzip. For one participant, heart_rate.json is about 79 MB, while most other files are under 15 MB.

## Layout per participant
- fitbit/: one file per metric. heart_rate.json, resting_heart_rate.json, time_in_heart_rate_zones.json, sleep.json, sleep_score.csv, steps.json, calories.json, distance.json, exercise.json, and daily minutes files (sedentary, lightly, moderately, very active)
- pmsys/: wellness.csv, srpe.csv, injury.csv (self-reported)
- googledocs/: reporting.csv (self-reported meals, weight, drinks)

## Observed formats and quirks
- heart_rate.json: one JSON list. Each item has dateTime (no time zone shown) and value with bpm and confidence. Roughly every 5 seconds, with gaps. Meaning of confidence still to verify.
- time_in_heart_rate_zones.json: one item per day, minutes per zone (IN_DEFAULT_ZONE_1 to 3, BELOW_DEFAULT_ZONE_1). Daily totals are near but usually below 1440 (distribution checked 2026-11-06). The first day can be partial.
- resting_heart_rate.json: some days hold placeholder rows with value 0.0 and a null date. Treat as missing.
- sleep.json: nested JSON. startTime uses a space format, endTime uses ISO with T and .000. duration is in milliseconds. Stage counts and minutes sit under levels.summary. dateOfSleep is the wake-up date.
- sleep_score.csv: timestamp ends in Z (UTC). Has sleep_log_entry_id, which looks like sleep.json logId (to verify before joining).
- wellness.csv: effective_time_frame in UTC. Ratings 1 to 5, readiness 0 to 10, soreness_area is a list stored as text.

## Plan for the pipeline
- Spark reads JSON lists with the multiLine option, flattens, writes Parquet.
- Join sleep tables on the log id, not on time.
- Do not join heart rate to UTC tables by time until time zones are checked.
- Small sample (16 people), so results illustrate the method, not general findings.

## Decisions made from data checks
- 2026-11-05: 5 of 1,747 wellness rows (0.3%) failed the rating range test. All were zeros on a 1 to 5 scale, none above the scale.
  Three rows had one zero rating. Two rows (both p05) were entirely zeros.
  Decision: treat a zero on a 1 to 5 rating as "not answered" (NULL). Flag all-zero entries as is_blank_entry and also NULL readiness and sleep duration.
  This is our interpretation of the pattern, not a statement from the dataset authors.
  Guard: a test fails if blank entries exceed 1% of rows.

## Corrections
- 2026-11-06: An earlier note said a zone key can be missing on some days. That came from a truncated file preview. After loading all 2,178 daily rows from 16 participants (2019-11-01 to 2020-03-31), no row has a missing zone key. The Spark job still records any_zone_missing as a guard.

## Resting heart rate coverage
- 2026-11-14: Only 14 of 16 participants have a resting_heart_rate.json file. p12 and p13 have no such file in their fitbit folder (confirmed by listing the folders). The reason is not documented in the dataset page.
- 145 of 1,803 rows are placeholders: heart rate 0 with a missing date. These two always occur together, with no exceptions.
- File sizes differ a lot between participants (p04 is about 4 KB, most are 17 to 19 KB), so days per participant will vary.
- 2026-11-14: p12 also has no lightly_active_minutes.json (the other participant folders checked so far have one). Not used yet.
