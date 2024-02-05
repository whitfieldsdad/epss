from epss.client import PolarsClient

client = PolarsClient(
    include_v1_scores=False,
    include_v2_scores=False,
    include_v3_scores=True,
)
min_date, max_date = client.get_date_range()

print(f'Min date: {min_date}')
print(f'Max date: {max_date}')
