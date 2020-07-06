from selenium import webdriver
from selenium.webdriver.support.ui import Select

from time import sleep

from zipfile import ZipFile
from glob import glob
from os import environ, remove, path, makedirs

import pandas as pd

from warnings import filterwarnings

class FFIECDataScraper:
    def __init__(self):
        filterwarnings("ignore", category = pd.errors.DtypeWarning)
        self.inputDir = environ['Input']
        self.outputDir = environ['Output']
        self.legend = {'RCON2200': 'Deposits',
                       'RCON2170': 'Assets',
                       'RCON2948': 'Liabilities',
                       'RIAD4107': 'Interest income',
                       'RIAD4073': 'Interest Expenses',
                       'RIAD4079': 'Non-interest income',
                       'RIAD4093': 'Non-interest expense'}
        return
    
    def run(self):
        self.download()
        self.merge()
    
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
        
        for column in self.legend.keys():
            df[column] = dataframe[column]
        
        return df
    
    def _cleanData(self, df):
        df = self._fixColumns(df)
        df = self._fixQuarter(df)
        
        return df
    
    def _fixColumns(self, df):
        df = df.drop(df.index[0])
        df = df.rename(columns=self.legend)
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
        merged.to_csv(path.join(self.outputDir, 'All banks.csv'), index=False)
        
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