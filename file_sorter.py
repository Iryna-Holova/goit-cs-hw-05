import argparse
import asyncio
import logging

from aiopath import AsyncPath
from aioshutil import copyfile


async def read_folder(path: AsyncPath, output: AsyncPath) -> None:
    """
    Recursively reads all files in the source folder and its subfolders.
    Copies files to corresponding subfolders in the output directory based on
    their extensions.
    """
    logging.info(f"Reading folder: {path}")
    async for file in path.rglob("*"):
        if await file.is_file():
            await copy_file(file, output)


async def copy_file(file: AsyncPath, output: AsyncPath) -> None:
    """
    Copies the file to the corresponding subfolder in the output directory
    based on its extension.
    """
    extension_name = file.suffix[1:]
    extension_folder = output / extension_name
    try:
        await extension_folder.mkdir(exist_ok=True, parents=True)
        await copyfile(file, extension_folder / file.name)
        logging.info(f"Copied {file.name} to {extension_folder}")
    except Exception as e:
        logging.error(f"Error copying {file.name}: {e}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(
        description="Sort files based on their extensions."
    )
    parser.add_argument("source", type=str, help="Source folder path")
    parser.add_argument("output", type=str, help="Output folder path")
    args = parser.parse_args()

    source = AsyncPath(args.source)
    output = AsyncPath(args.output)

    asyncio.run(read_folder(source, output))
