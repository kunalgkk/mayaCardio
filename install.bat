@echo off
echo === Installing MayaCardio Pipeline on Windows ===
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
echo === Installation Completed Successfully ===
echo Run 'pipeline.bat status' to verify.
