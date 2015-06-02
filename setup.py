from setuptools import find_packages, setup


def get_version():
    import imp
    import os

    with open(os.path.join('furious', '_pkg_meta.py'), 'rb') as f:
        mod = imp.load_source('_pkg_meta', 'biloba', f)

        return mod.version


setup_args = dict(
    name='furious',
    version=get_version(),
    license='Apache',
    description='Furious is a lightweight library that wraps Google App Engine'
                'taskqueues to make building dynamic workflows easy.',
    author='Robert Kluin',
    author_email='robert.kluin@workiva.com',
    url='http://github.com/Workiva/furious',
    packages=find_packages(exclude=['example']),
    download_url = "https://github.com/Workiva/furious/tarball/v1.3.0",
    keywords = ['async', 'gae', 'appengine', 'taskqueue'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
)

if __name__ == '__main__':
    setup(**setup_args)
