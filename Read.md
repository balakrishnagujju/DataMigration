Create a virtual environment:

python3 -m venv myenv

Activate the virtual environment:

source myenv/bin/activate


pip3 install simple-salesforce


deactivate / source deactivate


By using a virtual environment, you can manage project-specific dependencies more effectively without affecting the global Python installation on your system.

simple_salesforce
    api.py  ->  restful
        try:
            json_result = self.parse_result_to_json(result)
        except:
            return result.content
