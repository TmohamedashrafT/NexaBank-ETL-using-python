from typing import Set, Tuple
import string
from utilities.utils_function import load_english_words
class CaesarEncryptor:
    def encrypt(self, text: str, key: int) -> str:
        return self._shift_text(text, key)

    def decrypt(self, text: str, key: int) -> str:
        return self._shift_text(text, -key)

    def _shift_text(self, text: str, shift: int) -> str:
        result = ""
        for char in text:
            if char.isalpha():
                base = ord('A') if char.isupper() else ord('a')
                result += chr((ord(char) - base + shift) % 26 + base)
            else:
                result += char
        return result


class CaesarBruteForcer:
    def __init__(self, decryptor: CaesarEncryptor, english_words: Set[str]) -> None:
        self.decryptor = decryptor
        self.english_words = english_words

    def brute_force(self, cipher_text: str) -> Tuple[str, int]:
        max_valid = 0
        best_shift = 0
        best_decryption = ""

        for shift in range(1, 26):
            decrypted = self.decryptor.decrypt(cipher_text, shift)
            valid_count = self._count_valid_words(decrypted)

            if valid_count > max_valid:
                max_valid = valid_count
                best_shift = shift
                best_decryption = decrypted
        return best_decryption, best_shift

    def _count_valid_words(self, text: str) -> int:
        return sum(
            word.strip(string.punctuation).lower() in self.english_words
               for word in text.split()
        )

if __name__ == "__main__":
    encryptor = CaesarEncryptor()
    text = "Hello world!"
    shift = 5

    encrypted = encryptor.encrypt(text, shift)
    print("Encrypted:", encrypted)

    decrypted = encryptor.decrypt(encrypted, shift)
    print("Decrypted:", decrypted)

    english_words = load_english_words("english_words.txt")
    brute_forcer = CaesarBruteForcer(encryptor, english_words)
    guessed_text, guessed_key = brute_forcer.brute_force(encrypted)
    print("Brute Forced:", guessed_text, "| Key:", guessed_key)