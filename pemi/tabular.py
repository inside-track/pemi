import os
import sys
from pandas import pandas as pd
from tabulate import tabulate as tbl

_PATH = os.path.abspath(__file__)
_DIR_PATH = os.path.dirname(_PATH)
sys.path.append(_DIR_PATH)


class PemiTabular:
    __DEFAULT_FILE_DIR = 'pemi-tabular/'
    __DEFAULT_FILE_NAME = 'tabular.html'
    __CSS_FILE_PATH = _DIR_PATH + '/pemi-web/css/pemi_errors.css'

    def __init__(self, file_dir=__DEFAULT_FILE_DIR):
        super().__init__()
        self.__file_dir = file_dir
        self.__css_style = self.__load_css_file()
        self.__data = []
        self.__output_file = None

    @staticmethod
    def to_html_table(df, title):
        try:
            if not isinstance(df, pd.core.frame.DataFrame):
                raise ValueError
        except ValueError:
            raise ValueError("df argument should have a type of pandas.core.frame.DataFrame")

        html_title = "<h3 style=\"text-align: center;\">{}</h3> \n".format(title)
        df = df.applymap(str)
        df.replace("<", "&lt;", regex=True, inplace=True)
        df.replace(">", "&gt;", regex=True, inplace=True)
        html_table = html_title + tbl(df, list(df.columns.values), 'html', stralign='center')
        return html_table

    def add(self, df, df_name=None):
        if df_name is None:
            df_name = self.__generate_df_name()
        self.__data.append({
            "name": df_name,
            "df": df
        })

    def render(self, file=__DEFAULT_FILE_NAME):
        file_path = self.__file_dir + file
        self.__open(file_path)
        self.__append("<html><head>" + "<style>" + self.__css_style + "</style></head>")
        for _, item in enumerate(self.__data):
            html_table = PemiTabular.to_html_table(df=item['df'], title=item['name'])
            self.__append(html_table)
        self.__append("</html>")
        self.__close()

    def reset(self):
        self.__data = []

    def __append(self, data):
        self.__output_file.write(data)

    def __close(self):
        try:
            if self.__output_file is not None:
                self.__output_file.close()
        except IOError:
            raise IOError('Cannot close output file')

    def __open(self, file_path):
        self.__create_dir()

        try:
            self.__output_file = open(file_path, "w")
        except IOError:
            raise IOError('Cannot open file with path ' + file_path)

    def __generate_df_name(self):
        default_table_name = 'Pemi Errors Table {}'
        return default_table_name.format(len(self.__data) + 1)

    def __create_dir(self):
        try:
            if not os.path.exists(self.__file_dir):
                os.mkdir(self.__file_dir)
        except IOError:
            raise IOError('Cannot create directory ' + self.__file_dir)

    def __load_css_file(self):
        try:
            css_file = open(self.__CSS_FILE_PATH, "r")
            css_content = css_file.read()
        except IOError:
            raise IOError('Cannot find CSS file with path ' + self.__CSS_FILE_PATH)

        return css_content
