# TheProvisioner

TheProvisioner is a little multi-threading Python tool useful to insert a MySQL table with a number of rows, using a configurable number of threads.
Columns will be set to random data.
Strings will be initialised to random maximum data type length strings.


## Installation

No installation needed, just Python installed.


## Preparation

Install the following libraries. You can 

python3.6 -m venv oci-cli
source oci-cli/bin/activate

sudo yum install mysql-devel
pip install mysql-connector-python


## Usage

./theprovisioner.py  --host=\<HOSTNAME> -u\<USER> -P\<PORT> -p=\<PASSWORD> -d\<SCHEMA> -t\<TABLE> -r\<ROWS> -c\<CONCURRENCY>


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.


## License
[MIT](https://choosealicense.com/licenses/mit/)
