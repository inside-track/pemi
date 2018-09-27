from tabulate import tabulate as tbl
from pandas import pandas as pd
import os
import sys

path = os.path.abspath(__file__)
dir_path = os.path.dirname(path)
sys.path.append(dir_path)


class PemiTabular:
    __DEFAULT_FILE_DIR__ = 'pemi-tabular/'
    __DEFAULT_FILE_NAME__ = 'tabular.html'
    __file_dir__ = __DEFAULT_FILE_DIR__
    __output_file__ = None
    __css_style__ = None
    __data__ = []
    __CSS_FILE_PATH__ = dir_path + '/pemi-web/css/pemi_errors.css'

    def __init__(self, file_dir=__DEFAULT_FILE_DIR__):
        super().__init__()
        self.__file_dir__ = file_dir
        self.__css_style__ = self.__load_css_file__()

    def add(self, df, df_name=None):
        if df_name is None:
            df_name = self.__generate_df_name__()
        self.__data__.append({
            "name": df_name,
            "df": df
        })

    def render(self, file=__DEFAULT_FILE_NAME__):
        file_path = self.__file_dir__ + file
        self.__open__(file_path)
        self.__append__("<html><head>" + "<style>" + self.__css_style__ + "</style></head>")
        for i, item in enumerate(self.__data__):
            html_table = self.__to_html_table__(df=item['df'], title=item['name'])
            self.__append__(html_table)
        self.__append__("</html>")
        self.__close__()

    def __to_html_table__(self, df, title):
        try:
            if type(df) != pd.core.frame.DataFrame:
                raise ValueError
        except ValueError:
            raise ValueError("df argument should have a type of pandas.core.frame.DataFrame")

        html_title = "<h3 style=\"text-align: center;\">{}</h3> \n".format(title)
        df = df.applymap(str)
        df.replace("<", "&lt;", regex=True, inplace=True)
        df.replace(">", "&gt;", regex=True, inplace=True)
        html_table = html_title + tbl(df, list(df.columns.values), 'html', stralign='center')
        return html_table

    def __append__(self, data):
        self.__output_file__.write(data)

    def __close__(self):
        try:
            if self.__output_file__ is not None:
                self.__output_file__.close()
        except IOError:
            raise IOError('Cannot close output file')

    def __open__(self, file_path):
        self.__create_dir__()

        try:
            self.__output_file__ = open(file_path, "w")
        except IOError:
            raise IOError('Cannot open file with path ' + file_path)

    def __generate_df_name__(self):
        default_table_name = 'Pemi Errors Table {}'
        return default_table_name.format(len(self.__data__) + 1)

    def __create_dir__(self):
        try:
            if not os.path.exists(self.__file_dir__):
                os.mkdir(self.__file_dir__)
        except IOError:
            raise IOError('Cannot create directory ' + self.__file_dir__)

    def __load_css_file__(self):
        try:
            css_file = open(self.__CSS_FILE_PATH__, "r")
            css_content = css_file.read()
        except IOError:
            raise IOError('Cannot find CSS file with path ' + self.__CSS_FILE_PATH__)

        return css_content

    def reset(self):
        self.__data__ = []
