import sqlizer
import io
import os
import unittest


class TestSQLizerClient(unittest.TestCase):

    @staticmethod
    def _get_test_file(filename):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)

    def test_csv_conversion(self):
        file_content = io.BytesIO("""id,text,dates
            1,test1,2020-01-01
            2,test2,2020-02-02""".encode('utf8'))
        converter = sqlizer.File(file_content, sqlizer.DatabaseType.MySQL, sqlizer.FileType.CSV, 'test.csv', 'test')
        converter.convert(True)
        self.assertEquals(converter.status, sqlizer.ConversionStatus.Complete)
        result_sql = converter.download_result_file().text
        self.assertRegexpMatches(result_sql, "`id` INT")
        self.assertRegexpMatches(result_sql, "`text` VARCHAR\\(5\\)")
        self.assertRegexpMatches(result_sql, "`dates` DATETIME")

    def test_json_file_conversion(self):
        with open(self._get_test_file('test.json'), mode='r') as file_content:
            converter = sqlizer.File(file_content, sqlizer.DatabaseType.SQLServer, sqlizer.FileType.JSON, 'test.json', 'test')
            converter.convert(True)
            self.assertEquals(converter.status, sqlizer.ConversionStatus.Complete)
            result_sql = converter.download_result_file().text
            self.assertRegexpMatches(result_sql, "\\[prop1\\] INT")
            self.assertRegexpMatches(result_sql, "\\[prop2\\] INT")
            self.assertRegexpMatches(result_sql, "\\[prop3\\] NVARCHAR\\(4\\)")

    def test_invalid_json_file(self):
        file_content = io.BytesIO("""an invalid json file""".encode('utf8'))
        converter = sqlizer.File(file_content, sqlizer.DatabaseType.PostgreSQL, sqlizer.FileType.JSON, 'test.json', 'test')
        converter.convert(True)
        self.assertEquals(converter.status, sqlizer.ConversionStatus.Failed)
        self.assertIsNotNone(converter.message)
        self.assertIsNot(converter.message, "")
        
    def test_excel_conversion(self):
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test.xlsx'), mode='rb') as file_content:
            converter = sqlizer.File(file_content, sqlizer.DatabaseType.MySQL, sqlizer.FileType.XLSX, 'test.xlsx', 'test')
            converter.convert(True)
            self.assertEquals(converter.status, sqlizer.ConversionStatus.Complete)
            result_sql = converter.download_result_file().text
            self.assertRegexpMatches(result_sql, "`id` INT")
            self.assertRegexpMatches(result_sql, "`text` VARCHAR\\(5\\)")
            self.assertRegexpMatches(result_sql, "`dates` DATETIME")
    
        
    def test_excel_conversion_with_invalid_sheet_name(self):
        invalid_sheet_name = 'Sheet999'
        with open(self._get_test_file('test.xlsx'), mode='rb') as file_content:
            converter = sqlizer.File(file_content, sqlizer.DatabaseType.PostgreSQL, sqlizer.FileType.XLSX, 'test.xlsx', 'test', sheet_name=invalid_sheet_name)
            converter.convert(True)
            self.assertEquals(converter.status, sqlizer.ConversionStatus.Failed)
            self.assertIsNotNone(converter.message)
            self.assertRegexpMatches(converter.message, invalid_sheet_name)


if __name__ == '__main__':
    unittest.main()