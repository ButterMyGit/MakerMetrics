"use client";

import { memo } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

/** Assistant replies: full Markdown (tables, numbered lists, links), styled to match the app. */
export const Markdown = memo(function Markdown({ text }: { text: string }) {
  return (
    <div className="grid gap-2.5 text-sm leading-relaxed text-foreground">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          h1: (p) => <h3 className="mt-2 text-base font-semibold" {...p} />,
          h2: (p) => <h3 className="mt-2 text-base font-semibold" {...p} />,
          h3: (p) => <h4 className="mt-1.5 text-sm font-semibold" {...p} />,
          h4: (p) => <h4 className="mt-1.5 text-sm font-semibold" {...p} />,
          ul: (p) => <ul className="ml-5 list-disc space-y-1" {...p} />,
          ol: (p) => <ol className="ml-5 list-decimal space-y-1" {...p} />,
          a: (p) => (
            <a className="font-medium text-primary underline underline-offset-2" target="_blank" rel="noopener noreferrer" {...p} />
          ),
          hr: () => <hr className="my-1 border-border" />,
          blockquote: (p) => <blockquote className="border-l-2 pl-3 text-muted-foreground" {...p} />,
          code: ({ className, children, ...rest }) => (
            <code className={`rounded bg-muted px-1 py-0.5 text-[0.85em] ${className ?? ""}`} {...rest}>
              {children}
            </code>
          ),
          pre: (p) => <pre className="overflow-x-auto rounded-lg bg-muted p-3 text-xs [&>code]:bg-transparent [&>code]:p-0" {...p} />,
          table: (p) => (
            <div className="overflow-x-auto rounded-lg border">
              <table className="w-full border-collapse text-left text-sm" {...p} />
            </div>
          ),
          thead: (p) => <thead className="bg-muted/60 text-xs text-muted-foreground" {...p} />,
          th: (p) => <th className="whitespace-nowrap px-3 py-2 font-medium" {...p} />,
          td: (p) => <td className="border-t px-3 py-2 tabular-nums" {...p} />,
        }}
      >
        {text}
      </ReactMarkdown>
    </div>
  );
});
