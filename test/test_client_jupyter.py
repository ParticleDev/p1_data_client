import p1_data_client_python.helpers.git as git
import p1_data_client_python.helpers.unit_test as hut


class TestP1DataApiExampleNotebook(hut.TestCase):
    def test_notebook1(self) -> None:
        file_name = git.find_file_in_git_tree("p1_data_api_v1_example.ipynb")
        scratch_dir = self.get_scratch_space()
        hut.run_notebook(file_name, scratch_dir)


class TestP1DataClientExampleNotebook(hut.TestCase):
    def test_notebook1(self) -> None:
        file_name = git.find_file_in_git_tree("p1_data_client_example.ipynb")
        scratch_dir = self.get_scratch_space()
        hut.run_notebook(file_name, scratch_dir)


class TestP1EdgarDataClientExampleNotebook(hut.TestCase):
    def test_notebook1(self) -> None:
        file_name = git.find_file_in_git_tree(
            "p1_edgar_data_client_example.ipynb"
        )
        scratch_dir = self.get_scratch_space()
        hut.run_notebook(file_name, scratch_dir)
