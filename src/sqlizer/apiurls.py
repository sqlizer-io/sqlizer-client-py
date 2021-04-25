
class ApiUrls:
    SQLIZER_API_BASE_URL = 'https://sqlizer.io/api/'

    @staticmethod
    def Create():
        return '{base_url}files/'.format(base_url=ApiUrls.SQLIZER_API_BASE_URL)

    @staticmethod
    def UploadData(id, part_number=None):
        part_number_section = '?partNumber={part_number}'.format(part_number=part_number) if part_number is not None else None
        return '{base_url}files/{id}/data/{part_number_section}'.format(base_url=ApiUrls.SQLIZER_API_BASE_URL, id=id, part_number_section=part_number_section)

    @staticmethod
    def GetOrUpdate(id):
        return '{base_url}files/{id}/'.format(base_url=ApiUrls.SQLIZER_API_BASE_URL, id=id)