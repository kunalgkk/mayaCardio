from setuptools import setup, find_packages

setup(
    name="mayacardio",
    version="1.0.0",
    description="MayaCardio Bedside Data Pipeline - Never Lose a Heartbeat",
    author="M SATHWIK (VIT Bhopal)",
    packages=find_packages(),
    py_modules=["pipeline"],
    install_requires=[
        "fastapi>=0.110.0",
        "uvicorn>=0.28.0",
        "requests>=2.31.0",
        "psutil>=5.9.0",
    ],
    entry_points={
        "console_scripts": [
            "pipeline=mayacardio.cli:main",
        ],
    },
    python_requires=">=3.9",
)
