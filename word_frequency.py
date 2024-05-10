import string
import asyncio
import httpx
from collections import defaultdict, Counter
from matplotlib import pyplot as plt


async def get_text(url):
    """Fetch text from a given URL."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            if response.status_code == 200:
                return response.text
            else:
                print("Failed to fetch text from the URL.")
                return None
    except Exception as e:
        print(f"An error occurred while fetching text: {e}")
        return None


def remove_punctuation(text):
    """Remove punctuation from the text."""
    return text.translate(str.maketrans("", "", string.punctuation))


async def map_function(word) -> tuple:
    """Map each word to a tuple (word, 1)."""
    return word, 1


def shuffle_function(mapped_values):
    """Shuffle mapped values by grouping them based on the word."""
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


async def reduce_function(key_values):
    """Reduce the shuffled values by summing up the counts for each word."""
    key, values = key_values
    return key, sum(values)


async def map_reduce(text, search_words=None):
    """Execute the MapReduce operation on the text."""
    text = await get_text(url)
    if text:
        text = remove_punctuation(text)
        words = text.split()

        if search_words:
            words = [word for word in words if word in search_words]

        mapped_values = await asyncio.gather(
            *[map_function(word) for word in words]
        )

        shuffled_values = shuffle_function(mapped_values)

        reduced_values = await asyncio.gather(
            *[reduce_function(key_values) for key_values in shuffled_values]
        )

        return dict(reduced_values)
    else:
        return None


def visualize_top_words(result, top_n=10):
    """Visualize the top words with the highest frequency."""
    top_words = Counter(result).most_common(top_n)
    words, counts = zip(*top_words)
    plt.figure(figsize=(10, 6))
    plt.barh(words, counts, color='skyblue')
    plt.xlabel('Frequency')
    plt.ylabel('Words')
    plt.title(f'Top {top_n} Most Frequent Words')
    plt.gca().invert_yaxis()
    plt.show()


if __name__ == '__main__':
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"

    search_words = None  # Optionally, specify a list of search words
    result = asyncio.run(map_reduce(url, search_words))

    if result:
        print("Word frequency analysis result:", result)
        visualize_top_words(result)
    else:
        print("Failed to retrieve text from the URL.")
