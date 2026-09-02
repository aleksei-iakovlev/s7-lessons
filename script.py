import sys
from datetime import date as dt


def main():
    client_name = sys.argv[1]
    date = sys.argv[2]
    dir_name = sys.argv[3]

    if str(dt.today()) == date:
        print(f"Hi {client_name}!")
        current_dir = dir_name
        print(f'Your current directory is {current_dir}')
    else:
        print("Come back another day")


if __name__ == "__main__":
    main()

# python script.py 'alex' '2026-08-29' 'path/to/table/'
