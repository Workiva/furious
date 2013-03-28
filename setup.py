from distutils.core import setup, find_packages

setup(
    name='furious',
    version='0.1',
    license='Apache',
    description='Furious is a lightweight library that wraps Google App Engine'
                'taskqueues to make building dynamic workflows easy.',
    author='Robert Kluin',
    author_email='robert.kluin@webfilings.com',
    url='http://github.com/WebFilings/furious',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 2 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: Apache',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
)
