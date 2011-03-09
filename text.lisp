;; Copyright (c) 2011 Phil Hargett

;; Permission is hereby granted, free of charge, to any person obtaining a copy
;; of this software and associated documentation files (the "Software"), to deal
;; in the Software without restriction, including without limitation the rights
;; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
;; copies of the Software, and to permit persons to whom the Software is
;; furnished to do so, subject to the following conditions:

;; The above copyright notice and this permission notice shall be included in
;; all copies or substantial portions of the Software.

;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
;; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
;; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
;; THE SOFTWARE.

(in-package :hh-redblack)

;; ---------------------------------------------------------------------------------------------------------------------
;; file-based storage
;;
;; We're using a text-based file format.  Each file is structured as a sequence of forms, where each form is a single object 
;; (node or data), and the references between objects are in the form (form# offset) and the representation of such references
;; is always a fixed-size.  Accessing storage depends on all objects involved (both nodes and data) to have representations 
;; created by cl:print-object and readable by cl:read.
;;
;; The first form of the file is the header, which contains a version number intended to describe the version number
;; of the file format.  The last two forms of the file are the footer and it's backup (that is, a copy of the footer
;; used for consistency checks). The 2nd form in the file should contain the "leaf node" representation, which in an 
;; empty tree would also be the root.  All forms are terminated by #\Newline, although that also is a convenience, 
;; and not a required delimiter between forms (because the Lisp reader does not
;; require that)

;; TODO note that the use of 20-char wide columns is because (length (format nil (expt 2 64))) is 20

(defclass text-file-red-black-tree (persistent-red-black-tree)
  ((file-name :initarg :file-name :accessor file-name)
   (stream :initform nil :accessor storage-stream)
   (next-form-number :initform 0 :accessor next-form-number)))

(defclass storage-node ()
  ((left :initform nil :initarg :left :accessor left)
   (right :initform nil :initarg :right :accessor right)
   (color :initform nil :initarg :color :accessor color)
   (key :initform nil :initarg :key :accessor key)
   (data :initform nil :initarg :data :accessor data)))

(defmacro node (&key left right color key data)
  `(make-instance 'storage-node  :left ,left :right ,right :color ,color :key ,key :data ,data))

(defmethod print-object ((object storage-node) stream)
  (with-slots (left right color key data) object
    (format stream "~s" `(node :left ,left :right ,right :color ,color :key ,key :data ,data))))

(defclass storage-data ()
  ((content :initform nil :initarg :content :accessor content)))

(defmacro node-data (content)
  `(make-instance 'storage-data :content ',content))

(defmethod print-object ((data storage-data) stream)
  (with-slots (content) data
    (format stream "~s" `(data ,content))))

(defclass storage-location ()
  ((form-number :initform 0 :initarg :form :accessor form-number)
   (offset :initform 0 :initarg :offset :accessor offset)))

(defmacro loc (form offset)
  `(make-instance 'storage-location :form ,form :offset ,offset))

(defmethod print-object ((object storage-location) stream)
  (format stream "(LOC ~20<~s~> ~20<~s~>)" (form-number object) (offset object)))

(defclass storage-header ()
  ((version :initform 0 :initarg :version :accessor version)))

(defmacro header (version)
  `(make-instance 'storage-header :version ,version))

(defmethod print-object ((object storage-header) stream)
  (format stream "(HEADER ~20<~s~>)" (version object) ))

(defclass storage-footer ()
  ((leaf :initform (make-instance 'storage-location) :initarg :leaf :accessor leaf)
   (root :initform (make-instance 'storage-location) :initarg :root :accessor root)
   (next-form-number :initform 0 :initarg :next :accessor next-form-number)))

(defmacro footer (leaf-location root-location next)
  `(make-instance 'storage-footer :leaf ,leaf-location :root ,root-location :next ,next))

(defmethod print-object ((object storage-footer) stream)
  (format stream "(FOOTER ~s ~s ~20<~s~>)" (leaf object) (root object) (next-form-number object)))

(defgeneric equality (left right) 
  (:documentation "The equality test is important for detecting consistency of the footer and its backup")
  (:method ((left t) (right t))
    (equalp left right))
  (:method ((left storage-location) (right storage-location))
    (and (equal (form-number left) (form-number right))
	 (equal (offset left) (offset right))))
  (:method ((left storage-footer) (right storage-footer))
    (and (equality (root left) (root right))
	 (equality (leaf left) (leaf right))
	 (equality (next-form-number left) (next-form-number right)))))

(defclass storage-form ()
  ((form-number :initarg :number :accessor form-number)
   (contents :initarg :contents :accessor contents)))

(defmethod print-object ((object storage-form) stream)
  (format stream "(FORM ~20<~s~> ~s)" (form-number object) (contents object)))

(defmacro form (number contents)
  `(make-instance 'storage-form 
		   :number ,number 
		   :contents ,contents))

(defclass text-file-red-black-object (persistent-red-black-object)
  ((location :initform (make-instance 'storage-location) :initarg :location :accessor location)))

(defclass text-file-red-black-node (text-file-red-black-object persistent-red-black-node)
  ())

(defclass text-file-red-black-data (text-file-red-black-object persistent-red-black-data)
  ())

(defgeneric allocation-size (tree object)
  (:documentation "Size of the object persisted in the tree's storage; the units of measures for the size
   depend on the tree's storage implementation"))

;; ---------------------------------------------------------------------------------------------------------------------
;; implementation : Text-file storage
;; ---------------------------------------------------------------------------------------------------------------------

(define-condition inconsistent-storage (transaction-aborted)
  ()
  (:report (lambda (condition stream)
	     (declare (ignorable condition))
	     (format stream "Inconsistent storage: changes made before transaction could be committed."))))

(defmethod rb-node-class ((tree text-file-red-black-tree))
  'text-file-red-black-node)

(defmethod rb-data-class ((tree text-file-red-black-tree))
  'text-file-red-black-data)

(defun read-stored-object (stream)
  (let ((*package* (symbol-package 'red-black-tree)))
    (eval (read stream nil))))

(defun write-stored-object (stream object)
  (let ((*package* (symbol-package 'red-black-tree)))
    (format stream "~s~%" object))
  (finish-output stream))

(defun make-storage-header (tree)
  (make-instance 'storage-header ))

(defun make-storage-footer (tree)
  (make-instance 'storage-footer :root (location (root tree)) :leaf (location (leaf tree)) :next (next-form-number tree)))

(defmethod allocation-size ((tree text-file-red-black-tree) object)
  ;; note we're counting on storage-forms to always have the same
  ;; base allocation size, independent of the value of :number
  (let ((form (make-instance 'storage-form :number 1 :contents object)))
    (file-string-length (storage-stream tree) 
			(with-output-to-string (os)
			  (write-stored-object os form)))))

(defmethod allocation-size ((tree text-file-red-black-tree) (header storage-header))
  (length (with-output-to-string (os)
	    (write-stored-object os header))))

(defmethod allocation-size ((tree text-file-red-black-tree) (footer storage-footer))
  (length (with-output-to-string (os)
	    (write-stored-object os footer))))

(defun make-text-file-red-black-tree (file-name) ;; TODO consider having an argument for the tree class
  (let ((tree nil))
    (with-rb-transaction ((setf tree  (make-instance 'text-file-red-black-tree :file-name file-name)))
      tree)))

(defun open-storage-stream (tree)
  (setf (storage-stream tree)
	(open (file-name tree) :direction :io :if-exists :overwrite :if-does-not-exist :create :element-type 'character)))

(defun close-storage-stream (tree)
  (finish-output (storage-stream tree))
  (close (storage-stream tree)))

(defmethod prb-open-storage ((tree text-file-red-black-tree))
  "Open storage and move file-position to end of file for appending any new data"
  (labels ((initialize-storage (tree)
	     "Called the first time a storage file is used--just write out 'empty'
              header, because it will be rewritten soon"
	     (let ((header (make-storage-header tree))
		   (footer (make-storage-footer tree))
		   (stream (open-storage-stream tree)))
	       (file-position stream :start)
	       (write-stored-object stream header)
	       (let ((leaf-location (prb-location tree)))
	       	 (setf (leaf footer) leaf-location
	       	       (root footer) leaf-location)
		 (setf (slot-value (leaf tree) 'location) leaf-location))
	       (file-position stream :end)
	       (finish-output stream)))
	   (refresh-storage (tree footer)
	     "Refresh the storage object's slots from the provided footer"
	     (let ((leaf (make-instance (rb-node-class tree))))
	       ;; TODO this preparation of the leaf could be generalized
	       (setf (location leaf) (leaf footer)
		     (slot-value leaf 'left) leaf
		     (slot-value leaf 'right) leaf
		     (slot-value tree 'leaf) leaf
		     (slot-value tree 'root) leaf
		     (state leaf) :loaded))
	     ;; must be careful to reuse the leaf, in case root is the leaf sentinel (empty tree)
	     (unless (prb-leaf-location-p tree (root footer))
	       (let ((root (make-instance (rb-node-class tree))))
		 (setf (location root) (root footer))
		 (setf (state root) :unloaded
		       (slot-value tree 'root) root)))
	     (setf (next-form-number tree) (next-form-number footer))
	     ;; need to clear new root of transaction, since nothing has actually changed
	     (setf (new-root *rb-transaction*) nil)
	     tree)
	   (footer-location (tree)
	     (let ((stream (storage-stream tree)))
	       (- (file-length stream) (* 2 (allocation-size tree (make-storage-footer tree))))))
	   (recover-storage (tree)
	     "Check footer and backup for consistency, repairing if necessary; note that
              recovery should be idempotent, and always run"
	     ;; TODO a bit of a hack, but ensures there are no side effects
	     ;; from creating the tree object itself
	     (clear-changes)
	     ;; TODO consider an abort if hit an exception in here	     
	     (let* ((stream (open-storage-stream tree)))
	       (file-position stream (footer-location tree)) ;; set to expected location to read footer
	       (let ((footer (read-stored-object stream))
		     (backup (read-stored-object stream)))
		 (file-position stream :end) ;; restore file position, hopefully to end
		 (if (equality footer backup)
		     ;; intact; no recovery needed
		     (refresh-storage tree footer)
		     ;; they do not match; attempt to recover
		     (progn
		       (refresh-storage tree backup)
		       (prb-save-root tree (root tree)))))
	       (file-position stream :end))))
    (if (probe-file (file-name tree))
	(recover-storage tree)
	(initialize-storage tree))))

(defmethod prb-stash-node ((tree text-file-red-black-tree) left-location right-location color-value key-value data-location)
  (write-stored-object (storage-stream tree) 
		       (form (next-form-number tree) 
			     (node :left left-location 
				   :right right-location 
				   :color color-value 
				   :key key-value 
				   :data data-location)))
  (incf (next-form-number tree)))

(defmethod prb-fetch-node ((tree text-file-red-black-tree) location)
  (file-position (storage-stream tree) (offset location))
  (let ((form (read-stored-object (storage-stream tree))))
    (file-position (storage-stream tree) :end)
    (with-slots (left right color key data) (contents form)
      (values left right color key data))))

(defmethod prb-stash-data ((tree text-file-red-black-tree) contents)
  (write-stored-object (storage-stream tree)
		       (form (next-form-number tree)
			     (node-data contents)))
  (incf (next-form-number tree)))

(defmethod prb-fetch-data ((tree text-file-red-black-tree) location)
  (file-position (storage-stream tree) (offset location))
  (let ((form (read-stored-object (storage-stream tree))))
    (file-position (storage-stream tree) :end)
    (with-slots (contents) (contents form)
      contents)))

(defmethod prb-close-storage ((tree text-file-red-black-tree))
  (close-storage-stream tree))

(defmethod prb-location ((tree text-file-red-black-tree))
  (make-instance 'storage-location 
		 :form (next-form-number tree) 
		 :offset (file-position (storage-stream tree))))

(defmethod prb-leaf-location-p ((tree text-file-red-black-tree) location)
  (equality location 
	    (location (leaf tree))))

(defmethod prb-save-root ((tree text-file-red-black-tree) root)
  (let ((footer (make-storage-footer tree))
	(stream (open-storage-stream tree)))
    (file-position stream :end)
    (assert (not (equality (leaf footer) (loc 0 0))))
    (assert (not (equality (root footer) (loc 0 0))))
    (write-stored-object stream footer)
    (write-stored-object stream footer)
    (finish-output stream)
    (setf (state root) :unloaded
	  (slot-value tree 'root) root)))

