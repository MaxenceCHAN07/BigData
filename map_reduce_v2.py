{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyM+D8MQLxPnFECVRMeD43hy"
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "id": "yWVagh-PhbqE"
      },
      "outputs": [],
      "source": [
        "from mrjob.job import MRJob\n",
        "from mrjob.step import MRStep\n",
        "\n",
        "class UniqueTagCounts(MRJob):\n",
        "    def steps(self):\n",
        "        return [\n",
        "            MRStep(mapper=self.mapper_get_movie_tags,\n",
        "                   reducer=self.reducer_count_unique_tags)\n",
        "        ]\n",
        "\n",
        "    def mapper_get_movie_tags(self, _, line):\n",
        "        try:\n",
        "            userID, movieID, tag, timestamp = line.strip().split(',')\n",
        "            yield movieID, tag\n",
        "        except ValueError:\n",
        "            pass  # ignore malformed lines\n",
        "\n",
        "    def reducer_count_unique_tags(self, movieID, tags):\n",
        "        unique_tags = set(tags)\n",
        "        yield movieID, len(unique_tags)\n",
        "\n",
        "if __name__ == '__main__':\n",
        "    UniqueTagCounts.run()"
      ]
    }
  ]
}