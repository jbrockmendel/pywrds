try:
    from setuptools import setup, find_packages
except ImportError:
    import ez_setup
    ez_setup.use_setuptools()
    from setuptools import setup, find_packages

requirements = ['paramiko']

setup(
    name='pywrds',
    version='0.2.0',
    description='Python interface for WRDS data.',
    url='https://github.com/jbrockmendel/pywrds',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'install', 'notebooks']),
    install_requires = requirements,
    include_package_data=True,
)
