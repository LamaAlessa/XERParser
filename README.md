# XERParser
XER Files are proprietary file format used by Oracle Primavera P6 to store and exchange project schedule data.

They contain information such as:
- Project details
- Activities
- Relationships
- Resources
- Constrains

Although XER files are text-based, they're not designed to be easily readable by humans. Instead, they're optimized for system to system transfer 
between primavera P6 environments. 

Because of the richness of the data they contain, XER files can be used beyond scheduling tools. Once parsed, they enable:
- Building dashboards and reports
- Performing historical comparisons across projects
- Feeding schedule data into data pipelines and anyltical workflows 
