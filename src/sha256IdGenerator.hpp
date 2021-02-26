#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"
#include <fstream>
#include <thread>

static constexpr size_t HASH_LENGTH = 65;

class Sha256IdGenerator : public IdGenerator {
public:
    PageId generateId(std::string const& content) const
    {
        // Stworzenie komendy do wykonania.
        std::string command = "printf \"" + content + "\" | sha256sum";

        // Wykonanie polecenia i zapisanie wyniku
        FILE* tempfile = popen(command.c_str(), "r");

        if (tempfile == NULL)
            exit(1);

        // Odczytanie wyniku
        char hashed_content[HASH_LENGTH];
        if (fgets(hashed_content, HASH_LENGTH, tempfile) == NULL)
            exit(1);

        // Usunięcie temporalnego pliku.
        if (pclose(tempfile) == -1)
            exit(1);

        return PageId(std::string(hashed_content));
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
