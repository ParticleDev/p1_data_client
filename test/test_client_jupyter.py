import helpers.git as git
import helpers.unit_test as hut


class TestClientJupyterNotebook(hut.TestCase):
    def test_notebook1(self) -> None:
        file_name = git.find_file_in_git_tree("p1_data_client_example.ipynb")
        scratch_dir = self.get_scratch_space()
        hut.run_notebook(file_name, scratch_dir)
