# -------------------------------------------------------
#                       Matan Antebi
#                  Cell Phone: +9725229218
#         Email Address: matanantebi@mail.tau.ac.il
#                   Copyrights Reserved ©
# -------------------------------------------------------
from utils import *
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='main.py',
                                     description='Unemployment rate assignment.')
    parser.add_argument("Path", metavar="path", type=str, help="path define")
    parser.add_argument("seriesIDs", type=str, help="series id's.")
    parser.add_argument("startyear", type=str, help="start year.")
    parser.add_argument("endyear", type=str, help="end year.")
    parser.add_argument("Raw", action='store_true', help="save augmented images.")
    parser.add_argument("Mirror", action='store_true', help="save augmented images.")
    params = parser.parse_args()
    params = validate_args(params)
    main(params)


