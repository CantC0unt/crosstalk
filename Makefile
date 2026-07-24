.PHONY: install

install:
	@tmpdir=$$(mktemp -d); \
	trap 'rm -rf "$$tmpdir"' EXIT; \
	rsync -a --exclude '.git' --exclude 'build' --exclude '*.egg-info' ./ "$$tmpdir/"; \
	python3 -m pip install --user --upgrade "$$tmpdir" || exit $$?; \
	scripts_dir="$$(python3 -m site --user-base)/bin"; \
	case ":$$PATH:" in \
		*":$$scripts_dir:"*) printf '%s is already on PATH.\n' "$$scripts_dir" ;; \
		*) shell_name="$$(basename "$${SHELL:-}")"; \
			case "$$shell_name" in \
				zsh) shell_config="$${ZDOTDIR:-$$HOME}/.zshrc"; path_line='export PATH="'$$scripts_dir':$$PATH"' ;; \
				bash) shell_config="$$HOME/.bashrc"; path_line='export PATH="'$$scripts_dir':$$PATH"' ;; \
				fish) shell_config="$$HOME/.config/fish/config.fish"; path_line='fish_add_path "'$$scripts_dir'"'; mkdir -p "$$HOME/.config/fish" ;; \
				*) printf 'Python user scripts were installed in %s.\n' "$$scripts_dir"; \
					printf 'Shell %s is not configured automatically; add that directory to PATH for your shell.\n' "$${SHELL:-unknown}"; \
					exit 0 ;; \
			esac; \
			if [ -f "$$shell_config" ] && grep -Fqx "$$path_line" "$$shell_config"; then \
				printf '%s is already configured in %s for %s.\n' "$$scripts_dir" "$$shell_config" "$$shell_name"; \
			else \
				printf '\n# Python user-installed command-line tools\n%s\n' "$$path_line" >> "$$shell_config"; \
				printf 'Added %s to PATH in %s for %s. Restart your shell and Codex.\n' "$$scripts_dir" "$$shell_config" "$$shell_name"; \
			fi ;; \
	esac
