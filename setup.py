from distutils.core import setup

setup(
    name="nested-dict-fs",
    version="0.1",
    py_modules=['nested_dict_fs'],
    description="Permanent hierarchical storage using file-system directories with dict-like API",
    author="Liran Funaro",
    author_email="fonaro+nested_dict_fs@gmail.com",
    url="https://github.com/fonaro/nested-dict-fs",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    long_description=open('README').read(),
    install_requires=['lru-dict', 'numpy', 'msgpack', 'msgpack-numpy']
)

