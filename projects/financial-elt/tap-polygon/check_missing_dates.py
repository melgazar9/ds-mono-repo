import os
from datetime import date, timedelta
from optparse import OptionParser

from dateutil.easter import easter
from holidays.countries.united_states import UnitedStates


class USStockMarketHolidays(UnitedStates):
    def _populate(self, year):
        super()._populate(year)
        easter_sunday = easter(year)
        good_friday = easter_sunday - timedelta(days=2)
        self[good_friday] = "Good Friday"


def main():
    parser = OptionParser()
    parser.add_option("-d", "--subdir", dest="subdir", help="Subdirectory to process (e.g., 'SPY')", default="")
    parser.add_option(
        "-i",
        "--ignore",
        dest="ignore_list",
        help="Comma-separated list of directories or files to ignore within the subdirectory",
        default="",
    )

    (options, args) = parser.parse_args()

    base_dir = "outputs/us_stocks_sip/bars_1m"
    if options.subdir:
        base_dir = os.path.join(base_dir, options.subdir)

    ignore_items = [item.strip() for item in options.ignore_list.split(",")] if options.ignore_list else []

    start_date = date(2015, 5, 17)
    end_date = date(2025, 5, 17)

    us_holidays = USStockMarketHolidays()

    for root, dirs, files in os.walk(base_dir):
        if options.subdir and os.path.basename(root) == os.path.basename(base_dir):
            dirs[:] = [d for d in dirs if d not in ignore_items]
            files[:] = [f for f in files if f not in ignore_items]
        elif any(ignored_item in root for ignored_item in ignore_items if ignored_item in root):
            continue

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


if __name__ == "__main__":
    main()
