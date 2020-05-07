from mundi.plugin import utils


class TestPluginUtilities:
    def test_utility_functions(self):
        path = utils.find_data_path("mundi")
        assert str(path).endswith("data")
        assert path.exists()
        assert path.is_dir()

        sources = utils.find_data_sources("mundi")
        assert set(sources) == {"BR", "XX"}
        assert all(p.exists() for p in sources.values())

        scripts = utils.find_data_scripts("mundi")
        assert set(sources) == {"BR", "XX"}
        assert all(len(ps) >= 1 for ps in scripts.values())
        assert all(str(ps[0].name).startswith("prepare") == 1 for ps in scripts.values())
