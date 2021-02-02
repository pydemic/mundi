import mundi


class TestMundiFunctions:
    def test_country_id(self):
        assert mundi.country_id("BR") == "BR"
        assert mundi.country_id("br") == "BR"
        assert mundi.country_id("BRA") == "BR"
        assert mundi.country_id("bra") == "BR"
        assert mundi.country_id("076") == "BR"
        assert mundi.country_id("Brazil") == "BR"
        assert mundi.country_id("brazil") == "BR"

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
