from setuptools import setup
 
setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='GCP_data_eng_functions',
    url= 'https://github.com/JAPJ182/GCP_data_eng_functions',
    author='Jaime Polanco Ladan',
    author_email='jaime.polanco@javeriana.edu.co',
    # Needed to actually package something
    packages=['gdp_dataeng_functions'],
    # Needed for dependencies
    install_requires=['chardet==4.0.0',
                        'office365==0.3.15',
                        'Office365_REST_Python_Client', #==2.2.1,
                        'openpyxl==3.0.9',
                        'pandas==1.3.1',
                        'protobuf==3.19.4',
                        'pymongo==4.0.1',
                        'pytz==2021.1',
                        'requests==2.25.1',
                        'sendgrid==6.9.6',
                        'tqdm==4.61.2',
                        'patool==1.12' , 'numpy' ],
    # *strongly* suggested for sharing
    version='0.1',
    # The license can be anything you like
    license='Jaime Polanco Development',
    description='Ways for downloading the last file loaded into a specific bucket and way for loading this data into bigquery',
    # We will also need a readme eventually (there will be a warning)
    # long_description=open('README.txt').read(),
)
# if __name__ == "__main__":
#     setup()
