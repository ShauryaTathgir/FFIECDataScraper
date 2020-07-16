# del statements this is killing my computer lmao
# make csv dir before create connection
# add year limits, data cleaning options

from selenium import webdriver
from selenium.webdriver.support.ui import Select

from time import sleep

from zipfile import ZipFile
from glob import glob
from os import environ, remove, path, makedirs

import pandas as pd
from numpy import maximum

from warnings import filterwarnings

from TopBankSubsidiaries import TopBankSubsidiaries

import sqlite3
from sqlite3 import Error
import csv

class FFIECDataScraper:
    def __init__(self):
        filterwarnings("ignore", category = pd.errors.DtypeWarning)
        self.inputDir = environ['Input']
        self.outputDir = environ['Output']
        self.legend = {'RCON2200': 'Deposits',
                       'RIAD4107': 'Interest income',
                       'RIAD4073': 'Interest Expenses',
                       'RIAD4079': 'Non-interest income',
                       'RIAD4093': 'Non-interest expense'}
        
        self.assets = {'RCON2170': 'Assets',
                       'RCFD2170': 'Assets'}
        
        self.liabilities = {'RCFD2948': 'Liabilities',
                            'RCON2948': 'Liabilities'}
        
        self.banks, self.subsidiaries = TopBankSubsidiaries.getData()
        self.years = []
        
        for y in range(2001, 2021):
            self.years.append(y)
        
        return
    
    def run(self):
        self.download()
        self.merge()
        self.shorten()
    
    def download(self):
        self._accessWebsite()
        self._setReportType() 
        for year in self._getNumYears():
            self._downloadEveryYear(year)
        
        while(self._downloadInProgress()):
            sleep(5)
        
        self.driver.close()
        
        return
    
    def _accessWebsite(self):
        self.driver = webdriver.Chrome()
        self.driver.get("https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx")
        return
    
    def _setReportType(self):
        reportSelector = Select(self.driver.find_element_by_id('ListBox1'))
        reportSelector.select_by_value('ReportingSeriesSubsetSchedulesFourPeriods')
        sleep(0.5)
        return
    
    def _getNumYears(self):
        periods = Select(self.driver.find_element_by_id('DatesDropDownList'))
        return range(len(periods.options))
        
    def _downloadEveryYear(self, year):
        periods = Select(self.driver.find_element_by_id('DatesDropDownList'))
        periods.select_by_index(year)
        sleep(0.5)

        submit_button = self.driver.find_element_by_id('Download_0')
        submit_button.click()
        sleep(0.5)
        return
    
    def _downloadInProgress(self):
        return len(glob(path.join(self.inputDir, '*.crdownload'))) != 0
    
    def merge(self):
        self.unzip()
        self.combine()
        return
    
    def unzip(self):
        zippedFiles = glob(path.join(self.inputDir, 'FFIEC*.zip'))
        
        self._makeOutput()
        
        for folder in zippedFiles:
            self._extract(folder)
            remove(folder)
        
        sleep(1)
        return
    
    def _makeOutput(self):
        if not path.exists(self.outputDir):
            makedirs(self.outputDir)
        return
    
    def _extract(self, folder):
        with ZipFile(folder, 'r') as z:
            for file in z.namelist():
                if 'Readme' not in file:
                    z.extract(file, self.outputDir)
        return
    
    def combine(self):
        self._combineSingleYear()
        self._combineYears()
        return
    
    def _combineSingleYear(self):
        self._getRelevantYears()
        
        for year in self.years:
            filesInYear = glob(path.join(self.outputDir, '*' + str(year) + '*.txt'))
            self._combineAYear(filesInYear, str(year))
        
        return
    
    def _getRelevantYears(self):
        files = glob(path.join(self.outputDir, 'FFIEC*.txt'))
        
        self.years = set()
        
        for file in files:
            self.years.add(file[75:79])
        
        return
    
    def _combineAYear(self, files, year):
        dataframe = self._convertToDataframe(files)
        df = self._extractData(dataframe)
        df = self._cleanData(df)
        df = self._addYear(df, year)
        df.to_csv(path.join(self.outputDir, year + '.csv'), index = False)
        
        self._removeFiles(files)
        
        return
    
    def _convertToDataframe(self, files):
        dataframes = list()
        for file in files:
            df = pd.read_csv(file, delimiter='\t')
            dataframes.append(df)
        
        dataframe = pd.concat(dataframes, axis=1, sort=False)
        return dataframe
    
    def _extractData(self, dataframe):
        df = pd.DataFrame()
        df["Bank Name"] = dataframe.iloc[:, 6]
        df["Quarter"] = dataframe.iloc[:, 0]
        
        keys = list(self.assets.keys()) + list(self.liabilities.keys()) + list(self.legend.keys())
        
        for column in keys:
            df[column] = dataframe[column]
        
        return df
    
    def _cleanData(self, df):
        df = self._fixColumns(df)
        df = self._fixQuarter(df)
        df = self._rearrangeColumns(df)
        return df
    
    def _fixColumns(self, df):
        df = df.drop(df.index[0])
        
        df = self._combineDuplicates(df, list(self.assets.keys()), 'Assets')
        df = self._combineDuplicates(df, list(self.liabilities.keys()), 'Liabilities')
        
        df = df.rename(columns=self.legend)
        return df
    
    def _combineDuplicates(self, df, columns, name):
        series1, series2 = df[columns[0]], df[columns[1]]
        dframe1, dframe2 = series1.to_frame().astype(float), series2.to_frame().astype(float)
        dframe1.columns = [name]
        dframe2.columns = [name]
        dframe = dframe1.combine(dframe2, maximum, fill_value=-1)
        
        df = df.drop(columns=columns)
        df[name] = dframe[name]
        
        return df
    
    def _fixQuarter(self, df):
        q1 = list()
        q2 = list()
        q3 = list()
        q4 = list()
        
        for year in self.years:
            q1.append(str(year) + '-03-31')
            q2.append(str(year) + '-06-30')
            q3.append(str(year) + '-09-30')
            q4.append(str(year) + '-12-31')
        
        df = df.replace(q1, 'Q1')
        df = df.replace(q2, 'Q2')
        df = df.replace(q3, 'Q3')
        df = df.replace(q4, 'Q4')
        
        return df
    
    def _rearrangeColumns(self, df):
        columns = ['Bank Name', 'Quarter', 'Deposits', 'Assets', 'Liabilities', 'Interest income', 'Interest Expenses',
                 'Non-interest income', 'Non-interest expense']
        return df.reindex(columns=columns)
    
    def _removeFiles(self, files):
        for file in files:
            remove(file)
        return
    
    def _addYear(self, df, year):
        year = [year] * len(df.index)
        df.insert(1, 'Year', year)
        return df
    
    def _combineYears(self):
        files = self._getCsvs()
        dataframes = self._yearDataframes(files)
        merged = self._combineDataframes(dataframes)
        merged.to_csv(path.join(self.outputDir, 'FDIC Data.csv'), index=False)
        
        self._removeFiles(files)
        del files, dataframes
        return
    
    def _getCsvs(self):
        return glob(path.join(self.outputDir, '*.csv'))
    
    def _yearDataframes(self, files):
        dataframes = list()
        for file in files:
            dataframes.append(pd.read_csv(file))
        return dataframes
    
    def _combineDataframes(self, dataframes):
        merged = dataframes[0]
        
        for df in dataframes:
            if df is dataframes[0]:
                continue
            
            merged = merged.append(df, ignore_index=True)
        
        return merged
    
    def shorten(self):
        self._setUpDb()
        self._relevantBanks()
        self._combineEachCorp()
        self._combineCorps()
        return
    
    def _setUpDb(self):
        self.conn = self._createConnection()
        self.cursor = self.conn.cursor()
        self._createFDIC()
        return

    def _createConnection(self):
        """ create a database connection to a SQLite database """
        conn = None
        
        if not path.isdir(r'SQL/'):
            makedirs(r'SQL/')
        
        try:
            conn = sqlite3.connect(r'SQL/AllData.db')
        except Error as e:
            print(e)
        return conn
    
    def _createFDIC(self):
        try:
            self._createTable('FDIC')
            
            with open(path.join(self.outputDir, 'FDIC Data.csv'), 'r') as f:
                reader = csv.reader(f)
                next(reader)
                columns = ['BankName', 'Year', 'Quarter', 'Deposits', 'Assets', 'Liabilities',
                           'InterestIncome', 'InterestExpenses', 'NonInterestIncome', 'NonInterestExpense']
                query = 'insert into FDIC({0}) values ({1})'
                query = query.format(','.join(columns), ','.join('?' * len(columns)))
                for data in reader:
                    self.cursor.execute(query, data)
                self.conn.commit()
        except:
            pass
        
        return
    
    def _createTable(self, name):
        try:
            self.cursor.execute('''CREATE TABLE ''' + name + '''(
                                BankName varchar(100),
                                Year INT,
                                Quarter CHAR(2),
                                Deposits FLOAT,
                                Assets FLOAT,
                                Liabilities FLOAT,
                                InterestIncome FLOAT,
                                InterestExpenses FLOAT,
                                NonInterestIncome FLOAT,
                                NonInterestExpense FLOAT)''')
        except:
            pass
        return
    
    def _relevantBanks(self):
        corps = self.banks.keys()
        
        for corp in corps:
            self._createTable(corp)
            
            for bank in self.banks.get(corp):
                self._insertBankData(corp, bank)
        
        return

    def _insertBankData(self, corp, bank):
        baseQuery = "INSERT INTO {corp} SELECT * FROM FDIC WHERE BankName='{bank}'"
        
        try:
            bank = bank.replace("'", "%")
            query = baseQuery.format(corp=corp, bank=bank)
            self.cursor.execute(query)
            #self.conn.commit()
        except sqlite3.OperationalError:
            print(bank.replace("%", "'") + ' not working')

        return
    
    def _combineEachCorp(self):
        self._createTable('Summary')
        corps = self.banks.keys()
        baseQuery = "SELECT * FROM {corp} WHERE Year={year} AND Quarter='{quarter}'"
        
        for corp in corps:
            for year in self.years:
                for quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
                    query = baseQuery.format(corp=corp, year=year, quarter=quarter)
                    self.cursor.execute(query)
                    toBeCombined = self.cursor.fetchall()
                    
                    if toBeCombined:
                        data = [corp, year, quarter] + [0,] * 7
                        for row in toBeCombined:
                            for i in range(3, 10):
                                data[i] += row[i]
                    
                        self._addToSummary(data)
            self._deleteSubsidiaries(corp)
        return
    
    def _addToSummary(self, data):
        columns = ['BankName', 'Year', 'Quarter', 'Deposits', 'Assets', 'Liabilities',
                   'InterestIncome', 'InterestExpenses', 'NonInterestIncome', 'NonInterestExpense']
        query = 'insert into {0}({1}) values ({2})'
        query = query.format(data[0], ','.join(columns), ','.join('?' * len(columns)))
        self.cursor.execute(query, data)
        self.conn.commit()
        return
    
    def _deleteSubsidiaries(self, corp):
        query = "DELETE FROM {corp} WHERE BankName!='{corp}'".format(corp=corp)
        self.cursor.execute(query)
        self.conn.commit()
        return
    
    def _combineCorps(self):
        with open(path.join(self.outputDir, 'CondensedData.csv'), 'w', newline='') as f:
            condensedData = csv.writer(f, dialect='excel')
            condensedData.writerow(['Bank Name', 'Year', 'Quarter', 'Deposits', 'Assets', 'Liabilities',
                                    'Interest income', 'Interest Expenses', 'Non-interest income',
                                    'Non-interest expense'])
            
            for corp in self.banks.keys():
                data = self._getCorpData(corp)
                
                for row in data:
                    condensedData.writerow(row)
        return
    
    def _getCorpData(self, corp):
        query = "SELECT * FROM {corp}".format(corp=corp)
        self.cursor.execute(query)
        return self.cursor.fetchall()