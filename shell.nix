let
  pkgs = import <nixpkgs> {}; 
  unstable = import <nixpkgs-unstable> {};

  shell = pkgs.mkShell {
    buildInputs = [ unstable.emacs29
                    pkgs.leiningen
                    pkgs.graphviz
                    pkgs.sassc
                    pkgs.gnupg
                    pkgs.keychain
                    pkgs.maven
                    pkgs.jdk ];
  };  
in shell

