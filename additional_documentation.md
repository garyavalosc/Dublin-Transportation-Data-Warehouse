# Additional Documentation

## Approach and Rationale

The primary objective was to ingest and store raw data from various transportation sources into Snowflake. Here’s an outline of the approach and rationale behind certain decisions.

### Raw Data Storage

The raw data from each source is stored in Snowflake’s `RAW` schema. This approach enables us to keep an unmodified version of the data, which may be useful for historical comparisons or debugging. Storing raw data separately also allows for better flexibility if transformation requirements change in the future.

### Future Transformation and Modeling

The data is prepared for further transformation using a tool like dbt. After understanding client requirements (e.g., granularity, desired dimensions and metrics), views can be created with dbt to standardize and clean the data. This separation between raw ingestion and transformation aligns with the industry-standard ELT (Extract-Load-Transform) approach.

### Assumptions

1. **Schema for `RAW` Data**: We assumed a straightforward schema to load data without significant transformations. Some basic renaming and deduplication of columns were performed, but further transformations will be applied in the `CLEAN` or `MODEL` schema once requirements are finalized.

2. **Granularity and Requirements**: Detailed requirements regarding data granularity or specific calculations (e.g., aggregations or derived metrics) were deferred until further guidance from stakeholders. This allows the pipeline to be adaptable to evolving business needs.

3. **Error Handling**: Minimal error handling is included at this stage. Given the task's time constraints, additional error-checking layers (e.g., data validation, data quality checks) could be implemented as future improvements.

## Challenges Encountered

1. **Data Quality Issues**: Certain sources, such as Cycle Counts, included inconsistent or duplicate columns. To handle this, column deduplication logic was implemented, and non-alphanumeric characters were removed from column names to align with Snowflake naming conventions.


3. **Snowflake Identifier Limits**: Some column names exceeded Snowflake’s identifier length limits. A naming transformation was applied to ensure all column names fit within Snowflake's character limit and follow standardized conventions.

4. **Dynamic Schema Evolution**: The solution was designed to be extendable for future data sources. If additional data sources are added, the ETL functions in `transport_etl.py` can be extended with minimal changes.

5. **Testing and Debugging**: Given the time constraints, end-to-end testing was limited. Future improvements could involve automated tests or data validation checks at each pipeline stage.

---

This documentation is intended to guide reviewers through the approach, assumptions, and challenges involved in this project. Any questions or feedback are welcome for future iterations and improvements.
