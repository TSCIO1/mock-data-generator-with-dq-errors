import pandas as pd
import random
import re
from string import ascii_lowercase, digits

class MockDataGen:

    def __init__(self, row_count, invalid_percentage=None):

        self.row_count = row_count
        self.invalid_perc = invalid_percentage

        self.customer_id = [i for i in range(self.row_count)]
        self.first_names = self.gen_first_names()
        self.last_names = self.gen_last_names()
        self.initials = [i[0] for i in self.first_names]
        self.birth_dates = self.gen_birth_dates()
        self.phone_numbers = self.gen_phone_numbers()
        self.emails = self.gen_emails()

        d = {
            'customerId': self.customer_id, 'firstName':self.first_names, 'surname':self.last_names,
            'initials': self.initials, 'birthDate':self.birth_dates, 'phoneNumber':self.phone_numbers,
            'email':self.emails
        }
        self.df = pd.DataFrame(data=d)

        if invalid_percentage:
            pass


    def gen_first_names(self):
        df = pd.read_csv(r'/Users/thomas.stewart/Documents/code/mock_data/mock_data_sources/patronymes.csv')
        names = [i for i in df.loc[df['count'] > 100]['patronyme']]
        return [random.choice(names) for i in range(self.row_count)]

    def gen_last_names(self):
        df = pd.read_csv(r'/Users/thomas.stewart/Documents/code/mock_data/mock_data_sources/prenom.csv')
        names = [i for i in df.loc[df['sum'] > 100]['prenom']]
        return [random.choice(names) for i in range(self.row_count)]

    def gen_birth_dates(self):
        birth_dates = []
        for i in range(self.row_count):
            year = f'{random.randint(1945,2006)}'
            month = f'{random.randint(1,12)}'
            day = f'{
            random.randint(0,28) if month == '2' 
            else random.randint(0,30) if month in ['4', '6', '9', '11']
            else random.randint(0,31)}'
            if int(month) < 10:
                month = '0' + month
            if int(day) < 10:
                day = '0' + day
            birth_dates.append(f'{day}/{month}/{year}')
        return birth_dates

    def gen_phone_numbers(self):
        return [f'+44{random.randint(1000000000, 9999999999)}' for i in range(self.row_count)]

    def gen_emails(self):
        domains = ['@gmail.com', '@hotmail.com', '@outlook.com', '@yahoo.com']
        emails = []
        for i in range(self.row_count):
            s = ''.join([random.choice(ascii_lowercase) for i in range(random.randint(6,10))])
            if random.randint(0,1):
                s += f'.{''.join([random.choice(ascii_lowercase) for i in range(random.randint(6,10))])}'
            if random.randint(0,2):
                s += f'{''.join([random.choice(digits) for i in range(random.randint(1,4))])}'
            s += random.choice(domains)
            emails.append(s)
        return emails

class DataScrambler:

    # Adds in data quality issues to a dataframe based on certain identifying features
    # i.e. tries to identify initials, ids etc.

    def __init__(self, df):
        self.df = df
        self.type_dict = self.identify_data()

    def identify_data(self):
        scramble_types = {'ids': [], 'strSpecChar': [], 'str': [], 'dates': []}
        special_character_regex = r'''[01234567890_\.@-]'''
        date_regex = r'''\d{2}(-|\/|\\|\.)\d{2}(-|\/|\\|\.)(\d{4}|\d{2})|(\d{4}|\d{2})(-|\/|\\|\.)\d{2}(-|\/|\\|\.)\d{2}'''
        for i in self.df.columns:
            current_column = [values for values in self.df[i]]
            if all([isinstance(value, int) for value in current_column]):
                scramble_types['ids'].append(i)
                continue
            elif len(['match' for value in current_column if re.match(date_regex, value)]) > len(current_column)/80:
                scramble_types['dates'].append(i)
            elif len(['match' for value in current_column if re.findall(special_character_regex, value)]) > len(current_column)/10:
                scramble_types['strSpecChar'].append(i)
            elif len(['match' for value in current_column if re.findall(special_character_regex, value)]) < len(current_column)/10:
                scramble_types['str'].append(i)
        return scramble_types

    def destroy_dq(self, percentage):
        random_constant = int(100/percentage)
        for k,v in self.type_dict.items():
            for column_name in v:
                values = [i for i in self.df[column_name]]
                if k == 'ids':
                    values = [1 if not random.randint(0, random_constant) else i for i in values]
                elif k == 'strSpecChar' or k == 'dates':
                    values = ['aaaaaaaaa' if not random.randint(0, random_constant) else i for i in values]
                elif k == 'str':
                    values = [f'{i}{random.randint(1,999)}' if not random.randint(0, random_constant) else i for i in values]
                #adding in nulls based on 10% or so
                values = [i if random.randint(0,9) else pd.NA for i in values]
                self.df[column_name] = values

path = ''
mock_data = MockDataGen(100000)
data_scrambler = DataScrambler(mock_data.df)
data_scrambler.df.to_csv(
    f'{path}/mock_data_repo/customer.csv',
    index=False)
data_scrambler.destroy_dq(10)
data_scrambler.df.to_csv(
    f'{path}/mock_data_repo/customer_10_percent_dq.csv',
                         index=False)
data_scrambler.destroy_dq(10)
data_scrambler.df.to_csv(
    f'{path}/mock_data_repo/customer_20_percent_dq.csv',
    index=False)
