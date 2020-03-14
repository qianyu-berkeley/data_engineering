set nocompatible              " required
filetype off                  " required

" set the runtime path to include Vundle and initialize
set rtp+=~/.vim/bundle/Vundle.vim
call vundle#begin()

" alternatively, pass a path where Vundle should install plugins
"call vundle#begin('~/some/path/here')

" let Vundle manage Vundle, required
Plugin 'gmarik/Vundle.vim'


" Plugin utilities
Plugin 'tpope/vim-fugitive' " plugin on GitHub repo
Plugin 'scrooloose/nerdtree' " file drawer, open with :NERDTreeToggle
Plugin 'kien/ctrlp.vim' "fuzzy find files
Plugin 'klen/python-mode' "python mode


" All of your Plugins must be added before the following line
call vundle#end()            " required
filetype plugin indent on    " required

" Automatix reloading of .vimrc
autocmd! bufwritepost .vimrc source %

" set a map leader for more key combo
let mapleader = ','

" Python env
let g:pymode_python = 'python3'
"set pythonthreehome=$HOME/anaconda3
"set pythonthreedll=$HOME/anaconda3/lib/libpython3.6m.dylib
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
" Key Map
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

" bind Ctrl+<movement> keys to move around the windows, instead of using Ctrl+w + <movement>
map <c-j> <c-w>j
map <c-k> <c-w>k
map <c-l> <c-w>l
map <c-h> <c-w>h

" easier moving between tabs
map <Leader>n <esc>:tabprevious<CR>
map <Leader>m <esc>:tabnext<CR>

" map sort function to a key
vnoremap <Leader>s :sort<CR>

" easier moving of code blocks
" Try to go into visual mode (v), thenselect several lines of code here and
" then press ``>`` several times.
"vnoremap < <gv  " better indentation
"vnoremap > >gv  " better indentation
vnoremap <S-TAB> <gv  " better indentation
vnoremap <Tab> >gv  " better indentation

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
" Settings
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

" Better copy & paste
" When you want to paste large blocks of code into vim, press F2 before you
" paste. At the bottom you should see ``-- INSERT (paste) --``."
set pastetoggle=<F2>
set clipboard=unnamed

" Enable folding
set foldmethod=indent
set foldlevel=99

" Syntax highlighting
syntax on 

" line number on
set number

" Set a color column at 80
" set colorcolumn=80

" indentation 
set autoindent
set smartindent
set tabstop=4 " the visible width of tabs
set softtabstop=4 " edit as if the tabs are 4 characters wide
set shiftwidth=4
set shiftround " round indent to a multiple of 'shiftwidth'

" show the satus line all the time
set laststatus=2 

" Set font size
set guifont=Monaco:h14

" set history and undo
set history=1000
set undolevels=1000

" search
set hlsearch
set incsearch
set ignorecase
set smartcase

" Text width
set tw=80
set nowrap

" regex
set magic

"opening a new file when the current buffer has unsaved changes 
" causes files to be hidden instead of closed
set hidden

set wildmenu
set showcmd

" my color scheme
colorscheme monokai

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
" Plugin settings
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

" close NERDTree after a file is opened
let g:NERDTreeQuitOnOpen=0
" show hidden files in NERDTree
let NERDTreeShowHidden=1
" Toggle NERDTree
nmap <Leader>t :NERDTreeToggle<cr>

