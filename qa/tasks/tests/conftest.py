import glob


def pytest_addoption(parser):
    parser.addoption("--suite-dir", help="suite dir")

def pytest_generate_tests(metafunc):
    if "yaml_file" in metafunc.fixturenames:
        suite_dir = metafunc.config.getoption("--suite-dir")
        files = glob.glob(f"{suite_dir}/**/*.yaml", recursive=True)

        metafunc.parametrize("yaml_file", list(set(files)))
