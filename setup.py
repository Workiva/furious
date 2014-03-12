from setuptools import find_packages, setup


setup_args = dict(
    name='furious',
    version='0.9.5',
    license='Apache',
    description='Furious is a lightweight library that wraps Google App Engine'
                'taskqueues to make building dynamic workflows easy.',
    author='Robert Kluin',
    author_email='robert.kluin@webfilings.com',
    url='http://github.com/WebFilings/furious',
    packages=find_packages(exclude=['example']),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: Apache',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
)


if __name__ == '__main__':
    setup(**setup_args)
