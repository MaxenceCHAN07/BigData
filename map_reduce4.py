{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyNrbdfG0+jsLXOFO7C282DR"
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
        "from mrjob.job import MRJob\n",
        "from mrjob.step import MRStep\n",
        "\n",
        "class TagCounts(MRJob):\n",
        "\n",
        "    def steps(self):\n",
        "        return [MRStep(mapper=self.mapper_get_tags,\n",
        "                   reducer=self.reducer_count_tags)]\n",
        "\n",
        "    def mapper_get_tags(self, _, line):\n",
        "        try:\n",
        "            userID, movieID, tag, timestamp = line.split(',')\n",
        "            yield tag, 1\n",
        "        except Exception:\n",
        "            pass\n",
        "\n",
        "    def reducer_count_tags(self, tag, counts):\n",
        "        yield tag, sum(counts)\n",
        "\n",
        "if __name__ == '__main__':\n",
        "    TagCounts.run()"
      ]
    }
  ]
}