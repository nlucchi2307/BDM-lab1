import os
from dotenv import load_dotenv

from data_generator.generator import DataGenerator
from data_loader.loader import Loader

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

def main():
    dg = DataGenerator()

    print("Running Data Generator")

    dg.run()

    print("Running Loader")

    loader = Loader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, wipe=True)
    loader.load(dg)
    loader.driver.close()

if __name__ == "__main__":
    main()