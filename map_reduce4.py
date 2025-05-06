{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyM530WdVjgxh1hRXXBOsiv4"
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
        "id": "5YYCd0ZXfMB5"
      },
      "outputs": [],
      "source": [
        "from __future__ import print_function\n",
        "from mrjob.job import MRJob\n",
        "from mrjob.step import MRStep\n",
        "class UniqueTagsPerUserPerFilm(MRJob):\n",
        "    def steps(self):\n",
        "        return [MRStep(mapper=self.mapper_get_user_film_tags,reducer=self.reducer_count_unique_tags)]\n",
        "    def mapper_get_user_film_tags(self, _, line):\n",
        "        try:\n",
        "            userID, movieId, tag, timestamp = line.strip().split(',')\n",
        "            key = \"{}_{}\".format(userID, movieId)  # Compatible with Python 2\n",
        "            yield key, tag\n",
        "        except ValueError:\n",
        "            pass  # Ignore bad lines\n",
        "    def reducer_count_unique_tags(self, user_film, tags):\n",
        "        unique_tags = set(tags)\n",
        "        yield user_film, len(unique_tags)\n",
        "if __name__ == '__main__':\n",
        "    UniqueTagsPerUserPerFilm.run()"
      ]
    }
  ]
}