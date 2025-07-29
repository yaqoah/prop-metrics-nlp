import zipfile
import os
from src.ingestion.config.constants import PARSED_DATA_PATH

def create_colab_package():
    
    print("[Cloud / Deployment] Creating package for Google Colab")
    
    with zipfile.ZipFile("colab_package.zip", "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk("src"):
            for file in files:
                if file.endswith(".py"):
                    filepath = os.path.join(root, file)
                    zf.write(filepath)
        
        for json_file in PARSED_DATA_PATH.glob('*.json'):
            zf.write(json_file, f'src/ingestion/config/data/parsed/{json_file.name}')

        if os.path.exists("database"):
            for root, _, files in os.walk("database"):
                for file in files:
                    filepath = os.path.join(root, file)
                    zf.write(filepath)
        
        if os.path.exists("requirements.txt"):
            zf.write("requirements.txt")
        if os.path.exists(".env"):
            zf.write('.env')
    
    print(f"Created colab_package.zip ({os.path.getsize('colab_package.zip') / 1e6:.1f} MB)")

if __name__ == "__main__":
    create_colab_package()