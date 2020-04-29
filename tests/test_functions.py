import mundi


class TestMundiFunctions:
    def test_country_code(self):
        assert mundi.country_code("BR") == "BR"
        assert mundi.country_code("br") == "BR"
        assert mundi.country_code("BRA") == "BR"
        assert mundi.country_code("bra") == "BR"
        assert mundi.country_code("076") == "BR"
        assert mundi.country_code("Brazil") == "BR"
        assert mundi.country_code("brazil") == "BR"

    def test_code_function(self):
        assert mundi.code("Brazil") == "BR"
        assert mundi.code("brazil") == "BR"
        assert mundi.code("BR") == "BR"
        assert mundi.code("br") == "BR"
        assert mundi.code("BR-DF") == "BR-DF"
        assert mundi.code("BR/Distrito Federal") == "BR-DF"
        assert mundi.code("Brazil/Distrito Federal") == "BR-DF"
        assert mundi.code("brazil/distrito federal") == "BR-DF"
        assert mundi.code("Brazil/Brasília") == "BR-5300108"
