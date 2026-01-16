/* DigitScanner.h
 * 
 * Author           : Michael Kleber
 * Date Created     : 01/15/2026
 * Last Modified    : 01/15/2026
 * 
 */

#pragma once
#include "PublicLibs/Types.h"

namespace DigitViewer2 {
using namespace ymp;

class BasicDigitReader;

class DigitScanner {
public:
    DigitScanner(BasicDigitReader& reader, upL_t d);
    void search();

private:
    BasicDigitReader& m_reader;
    upL_t m_d;
};

}
