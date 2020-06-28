(TeX-add-style-hook
 "print"
 (lambda ()
   (TeX-add-to-alist 'LaTeX-provided-class-options
                     '(("article" "10pt")))
   (TeX-add-to-alist 'LaTeX-provided-package-options
                     '(("footmisc" "hang" "flushmargin") ("geometry" "paperheight=147mm" "paperwidth=49mm" "top=0mm" "bottom=1mm" "left=0mm" "right=2mm" "")))
   (TeX-run-style-hooks
    "latex2e"
    "article"
    "art10"
    "fontspec"
    "polyglossia"
    "footmisc"
    "graphicx"
    "geometry"))
 :latex)

