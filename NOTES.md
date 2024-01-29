# Notes

## Additional resources

- [EPSS data stats](https://www.first.org/epss/data_stats)

## Model updates

- No scores are available before 2021-04-14
- Output of the EPSS v2 (v2022.01.01) model is available from 2022-02-04 to 2023-03-06
- Output of the EPSS v3 (v2023-03-01) model is available from 2023-03-07 onward

## Growth in data volume over time

| Date | Total | Growth |
| ---- | ----- | ------ |
| 2022-01-01 | 79,547 | n/a |
| 2022-03-01 | 170,903 | 114.85% |
| 2022-06-01 | 177,195 | 3.68% |
| 2022-09-01 | 183,619 | 3.63% |
| 2023-01-01 | 192,048 | 4.59% |
| 2023-03-01 | 196,574 | 2.36% |
| 2023-06-01 | 203,872 | 3.71% |
| 2023-09-01 | 211,071 | 3.53% |
| 2024-01-01 | 220,901 | 4.66% |    

| Metric | Value |
| ------ | ----- |
| Min growth in score volume | 2.36% |
| Mean growth in score volume | 3.73% |
| Max<sub>1</sub> growth in score volume | 4.66% |

<sub>1. With outliers removed, scores were not available for all CVE IDs on 2022-01-01.</sub>

## Quantization

- Quantization consists of dropping EPSS scores that have not changed since the previous day
- Quantization resulted in a ~99.19% decrease in data volume (68,920,743 rows to 555,739 rows) as well as a representative decrease in the in-memory size of the resulting matrix (~2.65 GB to ~22 MB)
- The quantization process process took approximately 10 seconds to complete with Polars on a 2021 MacBook Pro with the Apple M1 Pro Chipset, including the time required to load the data in to memory from 328 different Parquet files containing the output of the EPSS v3 (v2023-03-01) model for 2023-03-07 to 2024-01-28.
