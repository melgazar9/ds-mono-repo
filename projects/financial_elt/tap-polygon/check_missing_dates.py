import os
from datetime import date, timedelta
import holidays
from holidays.countries.united_states import UnitedStates
from dateutil.easter import easter

class USStockMarketHolidays(UnitedStates):
    def _populate(self, year):
        super()._populate(year)
        # Add Good Friday (2 days before Easter Sunday)
        easter_sunday = easter(year)
        good_friday = easter_sunday - timedelta(days=2)
        self[good_friday] = "Good Friday"

base_dir = "outputs"
start_date = date(2015, 5, 17)
end_date = date(2025, 5, 17)


# us_holidays = holidays.US()
us_holidays = USStockMarketHolidays()

# Walk through all subdirectories under 'outputs/'
for root, dirs, files in os.walk(base_dir):
    if not files:
        continue

    folder_name = os.path.basename(root)
    file_set = set(files)
    missing = []

    for i in range((end_date - start_date).days + 1):
        day = start_date + timedelta(days=i)
        if day.weekday() >= 5 or day in us_holidays:
            continue

        expected_file = f"{folder_name}_{day}.csv.gz"
        if expected_file not in file_set:
            missing.append(expected_file)

    if missing:
        print(f"\n❌ Missing files in {root}:")
        for f in missing:
            print(f"  - {f}")
    else:
        print(f"\n✅ All files present in {root}")

