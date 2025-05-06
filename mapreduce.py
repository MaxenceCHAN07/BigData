{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyPlEtPoLvQIWsHiaDYhRJfi",
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
        "<a href=\"https://colab.research.google.com/github/MaxenceCHAN07/BigData/blob/main/mapreduce.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
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
        "!pip install mrjob\n",
        "# -*- coding: utf-8 -*-\n",
        "from mrjob.job import MRJob\n",
        "import csv\n",
        "\n",
        "class MRTagsCount(MRJob):\n",
        "\n",
        "    # Fonction Mapper\n",
        "    def mapper(self, _, line):\n",
        "        # Ignorer la ligne d'en-tête\n",
        "        if line.startswith(\"moveId\"):\n",
        "            return\n",
        "        # Charger chaque ligne CSV\n",
        "        reader = csv.DictReader([line])  # Lire la ligne comme un dictionnaire\n",
        "        for row in reader:\n",
        "            move_id = row['moveId']\n",
        "            # Émettre le couple clé-valeur (moveId, 1) pour chaque tag\n",
        "            yield (move_id, 1)\n",
        "\n",
        "    # Fonction Reducer\n",
        "    def reducer(self, move_id, counts):\n",
        "        # Calculer le total des tags par moveId\n",
        "        yield (move_id, sum(counts))\n",
        "\n",
        "\n",
        "if __name__ == '__main__':\n",
        "    MRTagsCount.run()"
      ]
    }
  ]
}