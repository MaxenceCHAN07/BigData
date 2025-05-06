{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyNIXmv3ODtOpwTA33Vv2PNI",
      "include_colab_link": true
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
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/MaxenceCHAN07/BigData/blob/main/mapreducee.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": 5,
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 106
        },
        "id": "NoemwISYvFib",
        "outputId": "08804f2f-ea5d-4728-d96e-69cf066d6a52"
      },
      "outputs": [
        {
          "output_type": "error",
          "ename": "SyntaxError",
          "evalue": "invalid syntax (<ipython-input-5-ba6e40ed86a6>, line 1)",
          "traceback": [
            "\u001b[0;36m  File \u001b[0;32m\"<ipython-input-5-ba6e40ed86a6>\"\u001b[0;36m, line \u001b[0;32m1\u001b[0m\n\u001b[0;31m    pip install mrjob\u001b[0m\n\u001b[0m        ^\u001b[0m\n\u001b[0;31mSyntaxError\u001b[0m\u001b[0;31m:\u001b[0m invalid syntax\n"
          ]
        }
      ],
      "source": [
        "from mrjob.job import MRJob\n",
        "from mrjob.step import MRStep\n",
        "\n",
        "class TagCounts(MRJob):\n",
        "    def steps(self):\n",
        "        return [\n",
        "            MRStep(mapper=self.mapper_get_tags,\n",
        "                   reducer=self.reducer_count_tags)\n",
        "        ]\n",
        "\n",
        "    def mapper_get_tags(self, _, line):\n",
        "        try:\n",
        "            userID, movieID, tag, timestamp = line.split(',')\n",
        "            yield movieID, 1\n",
        "        except Exception:\n",
        "            pass\n",
        "\n",
        "    def reducer_count_tags(self, movieID, counts):\n",
        "        yield movieID, sum(counts)\n",
        "\n",
        "if __name__ == '__main__':\n",
        "    TagCounts.run()"
      ]
    }
  ]
}