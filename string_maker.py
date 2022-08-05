import os
import pickle
root = os.path.dirname(__file__)
pickle_dir = f'{root}\\pickle'

def split(word):
    return [char for char in word]

if __name__ == '__main__':
    # Load pickles
    word_list = pickle.load(open(f'{pickle_dir}\\word_list.p', 'rb'))
    superscript_dict = pickle.load(open(f'{pickle_dir}\\superscript_dict.p', 'rb'))
    letters = split('acdehimortuvx')

    combined = ''
    superscript = 'erotic'
    for c in split(superscript):
        if c not in letters:
            print(f'{superscript} is not a letter')
            quit()
    lower = 'jordan'
    if len(superscript) != len(lower):
        print('Lengths do not match')
        quit()
    for idx, c in enumerate(lower):
        combined += (split(lower).pop(idx) + superscript_dict[split(superscript).pop(idx)])
    print(combined)
