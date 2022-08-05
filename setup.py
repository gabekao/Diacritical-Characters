import collections
import os
import pickle
from datetime import datetime 
root = os.path.dirname(__file__)
pickle_dir = f'{root}\\pickle'
txt_dir = f'{root}\\txt'

def dump_output(word_list, filename):
    with open(f'{txt_dir}\\{filename}.txt', 'w') as f:
        for word in word_list:
            f.write(f'{word}\n')

def generate_words():
    from nltk.corpus import words
    word_list = sorted([*set([word.lower() for word in words.words()])])
    dump_output(word_list, 'raw_list')
    pickle.dump(word_list, open(f'{pickle_dir}\\word_list.p', 'wb'))

def split(word):
    return [char for char in word]
    
def filter_down(chars, filter_list):
    # As Set
    filter = set()
    char = chars[0]
    for word in filter_list:
        if char not in word.lower():
            filter.add(word)
    chars.pop(0)
    if len(chars) != 0:
        filter = filter_down(chars, filter)
    return filter

def sort_list(word_set):
    sorted = collections.defaultdict(dict)
    for word in word_set:
        word = word.lower()
        if len(word) not in sorted.keys():
            sorted[len(word)][word[0]] = [word]
        if word[0] not in sorted[len(word)].keys():
            sorted[len(word)][word[0]] = [word]
        sorted[len(word)][word[0]].append(word)
    return sorted

if __name__ == '__main__':
    # # Generate Word List
    # generate_words()
    # quit()

    # Load pickles
    word_list = pickle.load(open(f'{pickle_dir}\\word_list.p', 'rb'))
    superscript_dict = pickle.load(open(f'{pickle_dir}\\superscript_dict.p', 'rb'))

    # Set letters
    alphabet = split('abcdefghijklmnopqrstuvwxyz')
    letters = split('acdehimortuvx')
    leftover = [char for char in alphabet if char not in letters]
    
    # Filter words
    start = datetime.now()
    filtered = sorted(filter_down(leftover, word_list), key=str.casefold)
    print(f'Time elapsed: {datetime.now() - start}')
    
    # Sort by length
    sorted = sort_list(filtered)
    pickle.dump(sorted, open(f'{pickle_dir}\\sorted_source.p', 'wb'))

    # Write results to file
    with open(f'{txt_dir}\\filtered_set.txt', 'w') as f:
        for num in range(1, max(sorted.keys())+1):
            f.write(f'{num}: {sorted[num]}\n')

    

