# Progress log

## 2026-11-02
- Downloaded PMData (16 participants, about 3 GB), kept it outside git and iCloud, linked at data/raw/pmdata
- Inspected layouts and formats, wrote docs/dataset.md
- Found quirks: missing zone keys, placeholder zero resting heart rates, mixed time formats, nested sleep JSON
- Learned: join on IDs when time zones are unclear, JSON lists need multiLine in Spark
- Next: load the first files into Snowflake
