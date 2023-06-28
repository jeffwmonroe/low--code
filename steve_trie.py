#!/usr/bin/python
# By Steve Hanov, 2011. Released to the public domain
import time
import sys
import pandas as pd

# Keep some interesting statistics
NODE_COUNT = 0
WORD_COUNT = 0


# The Trie data structure keeps a set of words, organized with one node for
# each letter. Each node has a branch for each letter that may follow it in the
# set of words.
class TrieNode:
    def __init__(self):
        self.value = None
        self.children = {}

        global NODE_COUNT
        NODE_COUNT += 1

    def insert(self, value):
        node = self
        id = value[0]
        word = value[1]
        for letter in word:
            if letter not in node.children:
                node.children[letter] = TrieNode()

            node = node.children[letter]

        node.value = value


# read dictionary file into a trie
def fill_trie(values):
    global WORD_COUNT
    global NODE_COUNT
    WORD_COUNT = 0
    NODE_COUNT = 0

    trie = TrieNode()
    for value in values:
        WORD_COUNT += 1
        # print(f'word = {word}')
        try:
            trie.insert(value)
        except TypeError:
            print(f"type error for: {value}")
    print(f"Read {WORD_COUNT} words into {NODE_COUNT} nodes")
    return trie


# The search function returns a list of all words that are less than the given
# maximum distance from the target word
def search(trie, word, max_cost):
    # build first row
    current_row = range(len(word) + 1)

    results = []
    best_result = None
    # recursively search each branch of the trie
    for letter in trie.children:
        best_result = search_recursive(trie.children[letter],
                                       letter,
                                       word,
                                       current_row,
                                       results,
                                       max_cost,
                                       best_result)

    return results, best_result


# This recursive helper is used by the search function above. It assumes that
# the previous_row has been filled in already.
def search_recursive(node, letter, word, previous_row, results, max_cost, best_result):
    columns = len(word) + 1
    current_row = [previous_row[0] + 1]

    # Build one row for the letter, with a column for each letter in the target
    # word, plus one for the empty string at column 0
    for column in range(1, columns):

        insert_cost = current_row[column - 1] + 1
        delete_cost = previous_row[column] + 1

        if word[column - 1] != letter:
            replace_cost = previous_row[column - 1] + 1
        else:
            replace_cost = previous_row[column - 1]

        current_row.append(min(insert_cost, delete_cost, replace_cost))

    # if the last entry in the row indicates the optimal cost is less than the
    # maximum cost, and there is a word in this trie node, then add it.
    if current_row[-1] <= max_cost and node.value is not None:
        new_result = (node.value[0], node.value[1], current_row[-1], (1 - current_row[-1] / len(node.value[1])) * 100.0)
        results.append(new_result)
        if best_result is None:
            best_result = new_result
        elif new_result[2] > new_result[2]:
            best_result = new_result

    # if any entries in the row are less than the maximum cost, then
    # recursively search each branch of the trie
    if min(current_row) <= max_cost:
        for letter in node.children:
            best_result = search_recursive(node.children[letter],
                                           letter,
                                           word,
                                           current_row,
                                           results,
                                           max_cost,
                                           best_result)
    return best_result


def get_best(trie, name, max_count):
    results, best = search(trie, name, max_count)
    return best, len(results)


def read_it_in():
    print('read_it_in')
    file_name = "actor_full.csv"
    # file_name = "actor_short.csv"
    df = pd.read_csv(file_name)
    print(df)

    external_file = "external_actor.csv"
    ext_df = pd.read_csv(external_file)

    return df, ext_df


def fuzzy_match(ontology_df, external_df):
    words = ontology_df['name'].tolist()
    keys = ontology_df['entity_id'].tolist()
    values = [item for item in zip(keys, words)]
    # print(values)
    trie = fill_trie(values)

    ext_names = external_df['external_name'].tolist()
    ext_ids = external_df['external_name'].tolist()
    external_values = [item for item in zip(ext_names, ext_ids)]
    results = []
    start = time.time()
    sum_results = 0
    count = 0
    matches = 0
    mapped_entity = {}
    for ext_name, ext_id in external_values:
        best, num_results = get_best(trie, ext_name, 3)
        sum_results += num_results
        count += 1
        new_row = {'external_id': ext_id,
                   'external_name': ext_name,
                   }
        if best is not None:
            matches += 1
            new_row['entity_id'] = best[0]
            new_row['name'] = best[1]
            new_row['confidence'] = best[3]
            mapped_entity[new_row['entity_id']] = True
        else:
            new_row['entity_id'] = None
            new_row['name'] = None
            new_row['confidence'] = 0
        results.append(new_row)
        if count % 100 == 0:
            print(f'[{count}] best: {ext_name}, {best}, {num_results}')
        if count > 3000:
            break
    avg_results = sum_results / len(ext_names)
    end = time.time()

    total_time = end - start
    print(f'total_time = {total_time}')
    print(f'total entries: {len(ext_names)}')
    print(f'average results = {avg_results}')
    print(f'percent matches = {matches / count}')

    for id, name in values:
        if id not in mapped_entity.keys():
            extra_row = {'entity_id': id,
                         'name': name,
                         'confidence': 0,
                         'external_id': None,
                         'external_name': None,
                         }
            results.append(extra_row)
    # for row in results:
    #     print(f'row = {row}')
    return results


def main():
    print('main')
    ontology_df, external_df = read_it_in()
    results = fuzzy_match(ontology_df, external_df)


if __name__ == "__main__":
    main()
